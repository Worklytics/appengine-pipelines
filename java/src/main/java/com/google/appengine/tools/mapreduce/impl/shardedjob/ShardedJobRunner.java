// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TransactionalTaskException;
import com.google.appengine.api.taskqueue.TransientFailureException;
import com.google.appengine.tools.mapreduce.RetryExecutor;
import com.google.appengine.tools.mapreduce.RetryUtils;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline.DeleteShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline.FinalizeShardedJob;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.apphosting.api.ApiProxy.ArgumentException;
import com.google.apphosting.api.ApiProxy.RequestTooLargeException;
import com.google.apphosting.api.ApiProxy.ResponseTooLargeException;
import com.google.apphosting.api.DeadlineExceededException;
import com.google.cloud.datastore.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Stream;

import static com.google.appengine.tools.mapreduce.RetryUtils.SYMBOLIC_FOREVER;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.*;
import static java.util.concurrent.Executors.callable;

/**
 * Contains all logic to manage and run sharded jobs; specific to a given backend configuration (injected as backend)
 *
 * @author ohler@google.com (Christian Ohler)
 *
 */
@AllArgsConstructor(onConstructor_ = @Inject)
@Log
public class ShardedJobRunner implements ShardedJobHandler {

  static final int TASK_LOOKUP_BATCH_SIZE = 20;

  @Getter
  final Provider<PipelineService> pipelineServiceProvider;
  @Getter
  final Datastore datastore;


  // High-level overview:
  //
  // A sharded job is started with a given number of tasks, task is invoked
  // over and over until it indicates it is complete.
  //
  // Each task is its own entity group to avoid contention.
  //
  // There is also a single entity (in its own entity group) that holds the
  // overall job state. It is updated only during initialization and when the tasks complete.
  //
  // Tasks entities carry a "sequence number" that allows it to detect if its work has already
  // been done (useful in case the task queue runs it twice). We schedule each
  // task in the same datastore transaction that updates the sequence number in
  // the entity.
  //
  // Each task also checks the job state entity to detect if the job has been
  // aborted or deleted, and terminates if so.

  private static RetryerBuilder baseRetryerBuilder() {
    return RetryerBuilder.newBuilder()
      .withWaitStrategy(RetryUtils.defaultWaitStrategy())
      .retryIfException(RetryUtils.handleDatastoreExceptionRetry())
      .retryIfExceptionOfType(ApiProxyException.class)
      .retryIfExceptionOfType(ConcurrentModificationException.class) // don't think this is thrown by new datastore lib
      .retryIfExceptionOfType(TransientFailureException.class)
      .retryIfExceptionOfType(TransactionalTaskException.class)
      .withRetryListener(RetryUtils.logRetry(log, ShardedJobRunner.class.getName()));
  }

  public static final RetryerBuilder FOREVER_RETRYER = baseRetryerBuilder().withStopStrategy(StopStrategies.stopAfterAttempt(SYMBOLIC_FOREVER));

  public static final RetryerBuilder FOREVER_AGGRESSIVE_RETRYER = baseRetryerBuilder()
    .retryIfException(e ->!(e instanceof RequestTooLargeException
      || e instanceof ResponseTooLargeException
      || e instanceof ArgumentException
      || e instanceof DeadlineExceededException))
    .withStopStrategy(StopStrategies.stopAfterAttempt(SYMBOLIC_FOREVER));


  public <T extends IncrementalTask> List<IncrementalTaskState<T>> lookupTasks(
    final ShardedJobRunId jobId, final int taskCount, final boolean lenient) {
    List<IncrementalTaskState<T>> taskStates = new ArrayList<>();
    Iterators.addAll(taskStates, lookupTasks(datastore, jobId, taskCount, lenient));
    return taskStates;
  }


  private <T extends IncrementalTask> ShardedJobStateImpl<T> lookupJobState(@NonNull Transaction tx, ShardedJobRunId jobId) {
    return (ShardedJobStateImpl<T>) Optional.ofNullable(tx.get(ShardedJobStateImpl.ShardedJobSerializer.makeKey(tx.getDatastore(), jobId)))
      .map(in -> ShardedJobStateImpl.ShardedJobSerializer.fromEntity(tx, in))
      .orElse(null);
  }

  @VisibleForTesting
  <T extends IncrementalTask> IncrementalTaskState<T> lookupTaskState(@NonNull Transaction tx, IncrementalTaskId taskId) {
    return (IncrementalTaskState<T>) Optional.ofNullable(tx.get(IncrementalTaskState.Serializer.makeKey(tx.getDatastore(), taskId)))
      .map(in -> IncrementalTaskState.Serializer.fromEntity(tx, in))
      .orElse(null);
  }




  @VisibleForTesting
  <T extends IncrementalTask> ShardRetryState<T> lookupShardRetryState(@NonNull Transaction tx, IncrementalTaskId taskId) {
    return (ShardRetryState<T>) Optional.ofNullable(tx.get(ShardRetryState.Serializer.makeKey(tx.getDatastore(), taskId)))
      .map(in -> ShardRetryState.Serializer.fromEntity(tx, in))
      .orElse(null);
  }

  private <T extends IncrementalTask> Iterator<IncrementalTaskState<T>> lookupTasks(
    @NonNull Datastore datastore, final ShardedJobRunId jobId, final int taskCount, final boolean lenient) {

    // does it in batches of 20, so prob not as slow as it seems ...
    return new AbstractIterator<>() {
      private int lastCount;
      private Iterator<Entity> lastBatch = Collections.emptyIterator();

      @Override
      protected IncrementalTaskState<T> computeNext() {
        if (lastBatch.hasNext()) {
          Entity entity = lastBatch.next();
          return IncrementalTaskState.Serializer.fromEntity(datastore, entity, lenient);
        } else if (lastCount >= taskCount) {
          return endOfData();
        }
        int toRead = Math.min(TASK_LOOKUP_BATCH_SIZE, taskCount - lastCount);
        List<Key> keys = new ArrayList<>(toRead);
        for (int i = 0; i < toRead; i++, lastCount++) {
          Key key = IncrementalTaskState.Serializer.makeKey(datastore, IncrementalTaskId.of(jobId, lastCount));
          keys.add(key);
        }
        TreeMap<Integer, Entity> ordered = new TreeMap<>();
        for (Iterator<Entity> it = datastore.get(keys.toArray(new Key[0])); it.hasNext(); ) {
          Entity entry = it.next();
          IncrementalTaskState state = IncrementalTaskState.Serializer.fromEntity(datastore, entry, false);
          ordered.put(state.getShardNumber(), entry);
        }
        lastBatch = ordered.values().iterator();
        return computeNext();
      }
    };
  }


  private <T extends IncrementalTask> void callCompleted(Datastore datastore, ShardedJobStateImpl<T> jobState) {
    Iterator<IncrementalTaskState<T>> taskStates =
      lookupTasks(datastore, jobState.getShardedJobId(), jobState.getTotalTaskCount(), false);
    Iterator<T> tasks = Iterators.transform(taskStates, IncrementalTaskState::getTask);
    jobState.getController().setPipelineService(pipelineServiceProvider.get());
    jobState.getController().completed(tasks);
  }

  private void scheduleControllerTask(ShardedJobRunId jobId, IncrementalTaskId taskId,
                                      ShardedJobSettings settings) {
    TaskOptions taskOptions = TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
      .url(settings.getControllerPath())
      .param(JOB_ID_PARAM, jobId.asEncodedString())
      .param(TASK_ID_PARAM, taskId.toString());
    taskOptions.header("Host", settings.getTaskQueueTarget());

    //Q: how can we transactionally add to queue with new library??
    //QueueFactory.getQueue(settings.getQueueName()).add(tx, taskOptions);
    QueueFactory.getQueue(settings.getQueueName()).add(taskOptions);
  }

  private <T extends IncrementalTask> void scheduleWorkerTask(ShardedJobSettings settings,
                                                              IncrementalTaskState<T> state, Long eta) {
    TaskOptions taskOptions = TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
      .url(settings.getWorkerPath())
      .param(TASK_ID_PARAM, state.getTaskId().toString())
      .param(JOB_ID_PARAM, state.getJobId().asEncodedString())
      .param(SEQUENCE_NUMBER_PARAM, String.valueOf(state.getSequenceNumber()));
    taskOptions.header("Host", settings.getTaskQueueTarget());
    if (eta != null) {
      taskOptions.etaMillis(eta);
    }
    //QueueFactory.getQueue(settings.getQueueName()).add(tx, taskOptions);
    //Q: how can we transactionally add to queue with new library??
    QueueFactory.getQueue(settings.getQueueName()).add(taskOptions);
  }

  @Override
  public void completeShard(@NonNull final ShardedJobRunId jobId, @NonNull final IncrementalTaskId taskId) {
    log.info("Polling task states for job " + jobId);
    PipelineService pipelineService = pipelineServiceProvider.get();

    //below seems to FAIL bc of transaction connection - why!?!?
    ShardedJobStateImpl<?> jobState = RetryExecutor.call(FOREVER_RETRYER, () -> {
      Transaction tx = getDatastore().newTransaction();
      try {
        ShardedJobStateImpl<?> jobState1 = lookupJobState(tx, jobId);
        if (jobState1 == null) {
          return null;
        }
        jobState1.setMostRecentUpdateTime(
          Stream.of(jobState1.getMostRecentUpdateTime(), Instant.now()).max(Comparator.naturalOrder()).get());

        //arguably, should be a function of deserializing jobState ...
        jobState1.getController().setPipelineService(pipelineService);

        jobState1.markShardCompleted(taskId.getNumber());

        if (jobState1.getActiveTaskCount() == 0 && jobState1.getStatus().isActive()) {
          jobState1.setStatus(new Status(DONE));
        }
        tx.put(ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, jobState1));
        tx.commit();
        return jobState1;
      } finally {
        rollbackIfActive(tx);
      }
    });

    if (jobState == null) {
      log.info(taskId + ": Job is gone, ignoring completeShard call.");
      return;
    }
    jobState.getController().setPipelineService(pipelineService);

    if (jobState.getActiveTaskCount() == 0) {
      if (jobState.getStatus().getStatusCode() == DONE) {
        log.info("Calling completed for " + jobId);
        // TODO(user): consider trying failed if completed failed after N attempts

        //q: should this be same txn as above??
        callCompleted(datastore, jobState);
      } else {
        log.info("Calling failed for " + jobId + ", status=" + jobState.getStatus());
        jobState.getController().failed(jobState.getStatus());
      }
      pipelineServiceProvider.get().startNewPipeline(
        new FinalizeShardedJob(datastore.getOptions(), jobId, jobState.getTotalTaskCount(), jobState.getStatus()));
    }
  }

  private <T extends IncrementalTask> IncrementalTaskState<T> getAndValidateTaskState(Transaction tx, IncrementalTaskId taskId,
                                                                                      int sequenceNumber, ShardedJobStateImpl<T> jobState) {
    IncrementalTaskState<T> taskState = lookupTaskState(tx, taskId);
    if (taskState == null) {
      log.warning(taskId + ": Task gone");
      return null;
    }
    if (!taskState.getStatus().isActive()) {
      log.info(taskId + ": Task no longer active: " + taskState);
      return null;
    }
    if (!jobState.getStatus().isActive()) {
      taskState.setStatus(new Status(StatusCode.ABORTED));
      log.info(taskId + ": Job no longer active: " + jobState + ", aborting task.");
      updateTask(tx, jobState, taskState, null, false);
      return null;
    }
    if (sequenceNumber == taskState.getSequenceNumber()) {
      if (!taskState.getLockInfo().isLocked()) {
        return taskState;
      }
      handleLockHeld(tx, taskId, jobState, taskState);
    } else if (taskState.getSequenceNumber() > sequenceNumber) {
      log.info(taskId + ": Task sequence number " + sequenceNumber + " already completed: "
        + taskState);
    } else {
      //q : throw here??
      log.severe(taskId + " sequenceNumber=" + sequenceNumber + " : Task state is from the past: " + taskState);
    }
    return null;
  }

  /**
   * Handle a locked slice case.
   */
  private <T extends IncrementalTask> void handleLockHeld(Transaction tx, IncrementalTaskId taskId, ShardedJobStateImpl<T> jobState,
                                                          IncrementalTaskState<T> taskState) {
    long currentTime = System.currentTimeMillis();
    int sliceTimeoutMillis = jobState.getSettings().getSliceTimeoutMillis();
    long lockExpiration = taskState.getLockInfo().lockedSince() + sliceTimeoutMillis;

    //NOTE: always 'false' now; requests that complete properly SHOULD release their locks..
    boolean wasRequestCompleted = wasRequestCompleted(taskState.getLockInfo().getRequestId());

    if (lockExpiration > currentTime && !wasRequestCompleted) {
      // if lock was not expired AND not abandon reschedule in 1 minute.
      long eta = Math.min(lockExpiration, currentTime + 60_000);
      scheduleWorkerTask(jobState.getSettings(), taskState, eta);
      log.info("Lock for " + taskId + " is being held. Will retry after " + (eta - currentTime));
    } else {
      ShardRetryState<T> retryState;
      if (wasRequestCompleted) {
        //request was completed, but lock was not released ??
        retryState = handleSliceFailure(tx, jobState, taskState, new RuntimeException(
          "Resuming after abandon lock for " + taskId + " on slice: "
            + taskState.getSequenceNumber()), true);
      } else {
        retryState = handleSliceFailure(tx, jobState, taskState, new RuntimeException(
          "Resuming after abandon lock for " + taskId + " on slice: "
            + taskState.getSequenceNumber() + "; lock held by request that never completed"), true);
      }
      updateTask(tx, jobState, taskState, retryState, false);
    }
  }

  /**
   * determines whether a given GAE request was completed by querying against Logs Service
   * @param requestId
   * @return whether request is known to have been completed
   */
  private static boolean wasRequestCompleted(String requestId) {
    if (requestId != null) {
      //previously, this checked against GAE LogService; this seems to no longer work as-expected
      // and there does not appear to be any clear success after we move from Java8 --> Java11 anyways
      // presumably, successor is Cloud Logging; neither REST or gRPC APIs provide any obvious way
      // to query by "request id"
      // @see https://cloud.google.com/logging/docs/apis
      // an actual Logs Explorer query that does it is:
      //   resource.type="gae_app" resource.labels.module_id="jobs"
      //   protoPayload.requestId="60db8a4400ff06d6adcf87f2400001737e6576616c2d656e67696e00016a6f62733a7633393863000100"
      // but this seems to be a BQ-powered search against partitioned log tables, not an efficient
      // lookup by id
      log.log(Level.INFO, "Check for whether request is completed no longer support; will assume it's not");
    }
    return false;
  }

  private <T extends IncrementalTask> boolean lockShard(Transaction tx,
                                                        IncrementalTaskState<T> taskState) {
    boolean locked = false;
    taskState.getLockInfo().lock();
    Entity entity = IncrementalTaskState.Serializer.toEntity(tx, taskState);
    try {
      tx.put(entity);
      locked = true;
    } finally {
      if (!locked) {
        taskState.getLockInfo().unlock();
      }
    }
    return locked;
  }

  @Override
  public void runTask(final ShardedJobRunId jobId, final IncrementalTaskId taskId, final int sequenceNumber) {
    //acquire lock (allows this process to START potentially long-running work of task itself)

    RetryExecutor.<Void>call(FOREVER_RETRYER, () -> {
      Transaction lockAcquisition = getDatastore().newTransaction();
      try {
        final ShardedJobStateImpl<? extends IncrementalTask> jobState = lookupJobState(lockAcquisition, jobId);

        if (jobState == null) {
          log.info(taskId + ": Job is gone, ignoring runTask call.");
          return null;
        }

        //taskState represents attempt of executing a slice of a shard of a sharded job
        IncrementalTaskState taskState =
          getAndValidateTaskState(lockAcquisition, taskId, sequenceNumber, jobState);
        if (taskState == null) {
          // some sort of error code happened

          // seems like getAndValidationTaskState has potential side-effects, which need to be committed
          lockAcquisition.commit();
          return null;
        }

        if (lockShard(lockAcquisition, taskState)) {
          // committing here, which forces acquisition of lock ...
          lockAcquisition.commit();

          // actual task execution
          runAndUpdateTask(jobState.getShardedJobId(), taskId, sequenceNumber, jobState, taskState);
        } else {
          log.warning("Failed to acquire the lock, Will reschedule task for: " + taskState.getJobId()
            + " on slice " + taskState.getSequenceNumber());
          long eta = System.currentTimeMillis() + new Random().nextInt(5000) + 5000;
          scheduleWorkerTask(jobState.getSettings(), taskState, eta);
        }
      } catch (ConcurrentModificationException ex) {
        // don't believe this is possible with new datastore lib
        throw new IllegalStateException("Concurrent modification exception should not happen here", ex);
      } finally {
        rollbackIfActive(lockAcquisition);
      }
      return null;
    });

  }

  private enum RetryType {
    SLICE,
    SHARD,
    JOB,
    NONE
  }

  //actual incremental task execution ( run() method )
  private <T extends IncrementalTask> void runAndUpdateTask(
    final ShardedJobRunId jobId,
                                                            final IncrementalTaskId taskId,
                                                            final int sequenceNumber,
                                                            final ShardedJobStateImpl<T> jobState,
                                                            IncrementalTaskState<T> taskState) {

    RetryType retryType = RetryType.NONE;
    Throwable t = null;
    //txn limited to 60s, so can't open this before run() call
    try {
      String statusUrl = jobState.getSettings().getPipelineStatusUrl();
      log.info("Running task " + taskId + " (job " + jobId + "), sequence number " + sequenceNumber
        + (statusUrl != null ? " Progress can be monitored at: " + statusUrl : ""));
      T task = taskState.getTask();
      try {
        task.prepare();
        task.run();
      } finally {
        task.cleanup();
      }
      if (task.isDone()) {
        taskState.setStatus(new Status(StatusCode.DONE));
      }

      // 2025-01 not clear on why clearing retry counts after successful run; why do
      // we want to obscure that has been retried??
      // but this is how FW historically worked, so leaving it
      taskState.clearRetryCount();
      taskState.setMostRecentUpdateTime(Instant.now());
    } catch (ShardFailureException ex ) {
      retryType = RetryType.SHARD;
      t = ex;
    } catch (JobFailureException ex) {
      retryType = RetryType.JOB;
      t = ex;
      log.log(Level.WARNING, "Shard " + taskState.getTaskId() + " triggered job failure", ex);
    } catch (RuntimeException ex) {
      t = ex;
      retryType = RetryType.SLICE;
    } catch (Throwable ex) {
      t = ex;
      retryType = RetryType.SLICE; // this was originally shard, but seems like a mistake
      log.log(Level.WARNING, "Slice encountered an Error.");
    } finally {

      RetryType finalRetryType = retryType;
      final RuntimeException toThrow;
      if (t instanceof RuntimeException) {
        toThrow = (RuntimeException) t;
      } else {
        toThrow = new RuntimeException(t);
      }
      RetryExecutor.call(FOREVER_RETRYER, () -> {
        Transaction postRunUpdate = getDatastore().newTransaction();
        try {
          ShardRetryState<T> retryState = null;

          switch (finalRetryType) {
            case SLICE:
              retryState = handleSliceFailure(postRunUpdate, jobState, taskState, toThrow, false);
              break;
            case SHARD:
              retryState = handleShardFailure(postRunUpdate, jobState, taskState, toThrow);
              break;
            case JOB:
              handleJobFailure(postRunUpdate, taskState, toThrow);
              break;
          }
          updateTask(postRunUpdate, jobState, taskState, retryState, true);
          postRunUpdate.commit();
        } catch (Throwable ex) {
          log.severe("Failed to write end of slice for task: " + taskState.getTask());
          // TODO(user): consider what to do here when this fail (though options are limited)
          throw ex;
        } finally {
          rollbackIfActive(postRunUpdate);
        }
        return null;
      });

    }

  }

  private <T extends IncrementalTask> ShardRetryState<T> handleSliceFailure(
    Transaction tx, ShardedJobStateImpl<T> jobState,
                                                                            IncrementalTaskState<T> taskState, RuntimeException ex, boolean failedDueToAbandonedLock) {
    if (ex instanceof RecoverableException || taskState.getTask().allowSliceRetry(failedDueToAbandonedLock)) {
      int attempts = taskState.incrementAndGetRetryCount();
      if (attempts > jobState.getSettings().getMaxSliceRetries()){
        log.log(Level.WARNING, "Slice exceeded its max attempts.");
        return handleShardFailure(tx, jobState, taskState, ex);
      } else {
        log.log(Level.INFO, "Slice attempt #" + attempts + " failed. Going to retry.", ex);
      }
      return null;
    } else {
      return handleShardFailure(tx, jobState, taskState, ex);
    }
  }

  private <T extends IncrementalTask> ShardRetryState<T> handleShardFailure(
    Transaction tx,
                                                                            ShardedJobStateImpl<T> jobState,
                                                                            IncrementalTaskState<T> taskState,
      Exception ex) {

    ShardRetryState<T> retryState = lookupShardRetryState(tx, taskState.getTaskId());

    if (retryState.incrementAndGet() > jobState.getSettings().getMaxShardRetries()) {
      log.log(Level.SEVERE, "Shard exceeded its max attempts, setting job state to ERROR.", ex);
      handleJobFailure(tx, taskState, ex);
    } else {
      // resets the slice of the shard; eg slice retry count --> 0, task back to initial task for shard

      log.log(Level.INFO,
        "Shard attempt #" + retryState.getRetryCount() + " failed. Going to retry.", ex);
      taskState.setTask(retryState.getInitialTask());
      taskState.clearRetryCount();
    }
    return retryState;
  }

  private <T extends IncrementalTask> void handleJobFailure(Transaction tx, IncrementalTaskState<T> taskState, Exception ex) {
    changeJobStatus(tx, taskState.getJobId(), new Status(ERROR, ex));
    taskState.setStatus(new Status(StatusCode.ERROR, ex));
    taskState.incrementAndGetRetryCount(); // trigger saving the last task instead of current
  }

  /**
   * updates task state for job IFF sequence number is the expected value; if not expected value, implies concurrent
   * execution and this update is ignored (eg, other execution wins); this leaves possibility that task's work executed
   * multiple times, in whole or in part.
   *
   * @param jobState        state of job under which task executing
   * @param taskState       to update
   * @param shardRetryState retry state of the shard
   * @param aggressiveRetry how aggressively to retry update
   */
  private <T extends IncrementalTask> void updateTask(
    final Transaction tx,
    final ShardedJobStateImpl<T> jobState,
    final IncrementalTaskState<T> taskState, /* Nullable */
    final ShardRetryState<T> shardRetryState,
    boolean aggressiveRetry) {

    // inc sequence number and release lock
    taskState.setSequenceNumber(taskState.getSequenceNumber() + 1);
    taskState.getLockInfo().unlock();

    @SuppressWarnings("rawtypes")
    RetryerBuilder exceptionHandler = aggressiveRetry ? FOREVER_AGGRESSIVE_RETRYER : FOREVER_RETRYER;
      // original code retries forever here?
      RetryExecutor.call(exceptionHandler,
        callable(new Runnable() {
          @Override
          public void run() {
            IncrementalTaskState<T> existing = lookupTaskState(tx, taskState.getTaskId());
            if (existing == null) {
              log.info(taskState.getTaskId() + ": Ignoring an update, as task disappeared while processing");
            } else if (existing.getSequenceNumber() != taskState.getSequenceNumber() - 1) {
              log.warning(taskState.getTaskId() + ": Ignoring an update, a concurrent execution changed it to: "
                + existing);
            } else {
              if (existing.getRetryCount() < taskState.getRetryCount()) {
                // Slice retry, we need to reset state
                taskState.setTask(existing.getTask());
              }
              writeTaskState(taskState, shardRetryState, tx);
              scheduleTask(jobState, taskState, tx);
            }
          }

          private void writeTaskState(IncrementalTaskState<T> taskState,
                                      ShardRetryState<T> shardRetryState, Transaction tx) {
            Entity taskStateEntity = IncrementalTaskState.Serializer.toEntity(tx, taskState);
            if (shardRetryState == null) {
              tx.put(taskStateEntity);
            } else {
              Entity retryStateEntity = ShardRetryState.Serializer.toEntity(tx, shardRetryState);
              tx.put(taskStateEntity, retryStateEntity);
            }
          }

          private void scheduleTask(ShardedJobStateImpl<T> jobState,
                                    IncrementalTaskState<T> taskState, Transaction tx) {
            if (taskState.getStatus().isActive()) {
              scheduleWorkerTask(jobState.getSettings(), taskState, null);
            } else {
              scheduleControllerTask(jobState.getShardedJobId(), taskState.getTaskId(),
                jobState.getSettings());
            }
          }
        }));
  }

  private <T extends IncrementalTask> void createTasks(Datastore datastore,
                                                       ShardedJobSettings settings,
                                                       ShardedJobRunId jobId,
                                                       List<? extends T> initialTasks,
                                                       Instant startTime) {
    log.info(jobId + ": Creating " + initialTasks.size() + " tasks");
    int taskNumber = 0;
    for (T initialTask : initialTasks) {
      // TODO(user): shardId (as known to WorkerShardTask) and taskId happen to be the same
      // number, just because they are created in the same order and happen to use their ordinal.
      // We should have way to inject the "shard-id" to the task.
      IncrementalTaskId taskId = IncrementalTaskId.of(jobId, taskNumber++);
      Transaction tx = datastore.newTransaction();
      try {
        IncrementalTaskState<T> taskState = lookupTaskState(tx, taskId);
        if (taskState != null) {
          log.info(jobId + ": Task already exists: " + taskState);
          continue;
        }
        taskState = IncrementalTaskState.create(taskId, jobId, startTime, initialTask);
        ShardRetryState<T> retryState = ShardRetryState.createFor(taskState);
        tx.put(IncrementalTaskState.Serializer.toEntity(tx, taskState),
          ShardRetryState.Serializer.toEntity(tx, retryState));
        scheduleWorkerTask(settings, taskState, null);
        tx.commit();
      } finally {
        rollbackIfActive(tx);
      }
    }
  }

  private <T extends IncrementalTask> void writeInitialJobState(Datastore datastore, ShardedJobStateImpl<T> jobState) {
    ShardedJobRunId jobId = jobState.getShardedJobId();
    Transaction tx = datastore.newTransaction();
    try {
      ShardedJobStateImpl<T> existing = lookupJobState(tx, jobId);
      if (existing == null) {
        tx.put(ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, jobState));

        log.info(jobId + ": Writing initial job state");
      } else {
        log.info(jobId + ": Ignoring Attempt to reinitialize job state: " + existing);
      }
      tx.commit();
    } finally {
      rollbackIfActive(tx);
    }
  }

  public <T extends IncrementalTask> void startJob(final ShardedJobRunId jobId, List<? extends T> initialTasks,
                                                   ShardedJobController<T> controller, ShardedJobSettings settings) {
    Instant startTime = Instant.now();
    Datastore datastore = getDatastore();
    Preconditions.checkArgument(!Iterables.any(initialTasks, Predicates.isNull()),
      "Task list must not contain null values");

    ShardedJobStateImpl<T> jobState =
      ShardedJobStateImpl.create(jobId, controller, settings, initialTasks.size(), startTime);
    if (initialTasks.isEmpty()) {
      log.info(jobId + ": No tasks, immediately complete: " + controller);
      jobState.setStatus(new Status(DONE));
      Transaction tx = datastore.newTransaction();
      try {
        tx.put(ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, jobState));
        tx.commit();
      } finally {
        rollbackIfActive(tx);
      }
      controller.setPipelineService(pipelineServiceProvider.get());
      controller.completed(Collections.emptyIterator());
    } else {
      writeInitialJobState(datastore, jobState);
      controller.setPipelineService(pipelineServiceProvider.get());
      createTasks(datastore, settings, jobId, initialTasks, startTime);
      log.info(jobId + ": All tasks were created");
    }
  }

  public ShardedJobState getJobState(ShardedJobRunId jobId) {
    Datastore datastore = getDatastore();
    Transaction tx = datastore.newTransaction();

    try {
      ShardedJobState state = Optional.ofNullable(datastore.get(ShardedJobStateImpl.ShardedJobSerializer.makeKey(datastore, jobId)))
        .map(in -> ShardedJobStateImpl.ShardedJobSerializer.fromEntity(tx, in, true))
        .orElse(null);
      tx.commit();
      return state;
    } finally {
      rollbackIfActive(tx);
    }
  }

  private void changeJobStatus(Transaction tx, ShardedJobRunId jobId, Status status) {
    log.info(jobId + ": Changing job status to " + status);

    ShardedJobStateImpl<?> jobState = lookupJobState(tx, jobId);
    if (jobState == null || !jobState.getStatus().isActive()) {
      log.info(jobId + ": Job not active, can't change its status: " + jobState);
      return;
    }
    jobState.setStatus(status);
    tx.put(ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, jobState));
  }

  private void rollbackIfActive(Transaction tx) {
    try {
      if (tx.isActive()) {
        tx.rollback();
      }
    } catch (RuntimeException e) {
      log.log(Level.WARNING, "Rollback of transaction failed: ", e);
    }
  }

  public void abortJob(ShardedJobRunId jobId) {
    RetryExecutor.call(FOREVER_RETRYER, () -> {
      Transaction tx = getDatastore().newTransaction();
      try {
        changeJobStatus(tx, jobId, new Status(ABORTED));
        tx.commit();
      } finally {
        rollbackIfActive(tx);
      }
      return null;
    });
  }


  public boolean cleanupJob(ShardedJobRunId jobId) {
    Transaction txn = datastore.newTransaction();
    ShardedJobStateImpl<?> jobState = lookupJobState(txn, jobId);
    if (jobState == null) {
      return true;
    }
    if (jobState.getStatus().isActive()) {
      return false;
    }
    int taskCount = jobState.getTotalTaskCount();
    if (taskCount > 0) {
      pipelineServiceProvider.get().startNewPipeline(new DeleteShardedJob(datastore.getOptions(), jobId, taskCount));
    }
    final Key jobKey = ShardedJobStateImpl.ShardedJobSerializer.makeKey(datastore, jobId);

    RetryExecutor.call(FOREVER_RETRYER, callable(() -> datastore.delete(jobKey)));
    return true;
  }
}
