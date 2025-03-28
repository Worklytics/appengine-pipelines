// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.appengine.tools.mapreduce.RetryExecutor;
import com.google.appengine.tools.mapreduce.RetryUtils;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline.DeleteShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline.FinalizeShardedJob;
import com.google.appengine.tools.mapreduce.servlets.ShufflerParams;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesService;
import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.tools.txn.PipelineBackendTransaction;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Uninterruptibles;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.appengine.tools.mapreduce.RetryUtils.SYMBOLIC_FOREVER;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.*;
import static java.util.concurrent.Executors.callable;

/**
 * Contains all logic to manage and run sharded jobs; specific to a given backend configuration (injected as backend)
 *
 * TODO: this is really coupled/equivalent to AppEngineBackend; should either merge with that *or* abstract this on top of that
 *
 * @author ohler@google.com (Christian Ohler)
 *
 */
@Log
public class ShardedJobRunner implements ShardedJobHandler {

  static final int TASK_LOOKUP_BATCH_SIZE = 20;

  /**
   * a status of an Incremental task; not to be confused with IncrementTaskState, which is a datastore entity
   * that encodes more state information / data about the task etc/
   */
  enum IncrementalTaskStatus {
    TASK_OK,
    TASK_GONE,
    TASK_NO_LONGER_ACTIVE,
    JOB_NO_LONGER_ACTIVE,
    TASK_STATE_FROM_PAST,
    TASK_ALREADY_COMPLETED,
    LOCK_HELD_BY_OTHER_EXECUTION,
    ;

    boolean passed() {
      return this == TASK_OK;
    }
  }
  @Getter
  private final Provider<PipelineService> pipelineServiceProvider;
  @Getter
  private final Datastore datastore;

  private final AppEngineServicesService appEngineServicesService;

  private final PipelineTaskQueue taskQueue;


  @Inject
  public ShardedJobRunner(
                          Provider<PipelineService> pipelineServiceProvider,
                          Datastore datastore,
                          AppEngineServicesService appEngineServicesService,
                          PipelineTaskQueue taskQueue) {
    this.pipelineServiceProvider = pipelineServiceProvider;
    this.datastore = datastore;
    this.appEngineServicesService = appEngineServicesService;
    this.taskQueue = taskQueue;
    if (System.getProperty("GOOGLE_CLOUD_PROJECT") != null) {
      // assume non testing environment
      DELAY_MULTIPLIER = 5;
    }
  }

  private int DELAY_MULTIPLIER = 1;

  @Getter @Setter
  private Duration controllerTaskDelay = Duration.ofSeconds(2).multipliedBy(DELAY_MULTIPLIER);
  @Getter @Setter
  private Duration workerTaskDelay = Duration.ofSeconds(2).multipliedBy(DELAY_MULTIPLIER);
  @Getter @Setter
  private Duration lockCheckTaskDelay = Duration.ofSeconds(60);


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
      // don't think this is thrown by new datastore lib
      // thrown by us if the task state is from the past
      .retryIfExceptionOfType(IllegalStateException.class)
      .withRetryListener(RetryUtils.logRetry(log, ShardedJobRunner.class.getName()));
  }

  public static final RetryerBuilder FOREVER_RETRYER = baseRetryerBuilder().withStopStrategy(StopStrategies.stopAfterAttempt(SYMBOLIC_FOREVER));

  public static final RetryerBuilder FOREVER_AGGRESSIVE_RETRYER = baseRetryerBuilder()
    .withStopStrategy(StopStrategies.stopAfterAttempt(SYMBOLIC_FOREVER));

  public <T extends IncrementalTask> List<IncrementalTaskState<T>> lookupTasks(
    final ShardedJobRunId jobId, final int taskCount, final boolean lenient) {
    List<IncrementalTaskState<T>> taskStates = new ArrayList<>();
    Iterators.addAll(taskStates, lookupTasks(datastore, jobId, taskCount, lenient));
    return taskStates;
  }


  private <T extends IncrementalTask> ShardedJobStateImpl<T> lookupJobState(@NonNull PipelineBackendTransaction tx, ShardedJobRunId jobId) {
    return (ShardedJobStateImpl<T>) Optional.ofNullable(tx.get(ShardedJobStateImpl.ShardedJobSerializer.makeKey(tx.getDatastore(), jobId)))
      .map(in -> ShardedJobStateImpl.ShardedJobSerializer.fromEntity(tx, in))
      .orElse(null);
  }

  @VisibleForTesting
  <T extends IncrementalTask> IncrementalTaskState<T> lookupTaskState(@NonNull PipelineBackendTransaction tx, IncrementalTaskId taskId) {
    return (IncrementalTaskState<T>) Optional.ofNullable(tx.get(IncrementalTaskState.makeKey(tx.getDatastore(), taskId)))
      .map(in -> IncrementalTaskState.Serializer.fromEntity(tx, in))
      .orElse(null);
  }




  @VisibleForTesting
  <T extends IncrementalTask> ShardRetryState<T> lookupShardRetryState(@NonNull PipelineBackendTransaction tx, IncrementalTaskId taskId) {
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
          Key key = IncrementalTaskState.makeKey(datastore, IncrementalTaskId.of(jobId, lastCount));
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

  @SneakyThrows
  private void scheduleControllerTask(ShardedJobRunId jobId, IncrementalTaskId taskId,
                                      ShardedJobSettings settings, PipelineBackendTransaction tx) {

    PipelineTaskQueue.TaskSpec.TaskSpecBuilder controllerTaskSpec = PipelineTaskQueue.TaskSpec.builder()
      .method(PipelineTaskQueue.TaskSpec.Method.POST)
      .callbackPath(settings.getControllerPath())
      .param(TASK_ID_PARAM, taskId.toString())
      .param(JOB_ID_PARAM, jobId.asEncodedString())
      .scheduledExecutionTime(Instant.now().plus(getControllerTaskDelay()));


    // used to be sent generically as value of a 'Host' header; I think this is clearer as usually expected
    // alternatively, could refactor to move worker service/version stuff down into PipelineTaskQueue, rather than doing mapping to host here??
    controllerTaskSpec.host(getWorkerServiceHostName(settings));

    tx.enqueue(settings.getQueueName(), controllerTaskSpec.build());
  }

  @SneakyThrows
  private <T extends IncrementalTask> void scheduleWorkerTask(ShardedJobSettings settings,
                                                              IncrementalTaskState<T> state,
                                                              Long etaMillis,
                                                              PipelineBackendTransaction tx) {


    PipelineTaskQueue.TaskSpec.TaskSpecBuilder workerTaskSpec = PipelineTaskQueue.TaskSpec.builder()
      .method(PipelineTaskQueue.TaskSpec.Method.POST)
      .callbackPath(settings.getWorkerPath())
      .param(TASK_ID_PARAM, state.getTaskId().toString())
      .param(JOB_ID_PARAM, state.getJobId().asEncodedString())
      .param(SEQUENCE_NUMBER_PARAM, String.valueOf(state.getSequenceNumber()));

    // used to be sent generically as value of a 'Host' header; I think this is clearer as usually expected
    // alternatively, could refactor to move worker service/version stuff down into PipelineTaskQueue, rather than doing mapping to host here??
    workerTaskSpec.host(getWorkerServiceHostName(settings));

    if (etaMillis != null) {
      workerTaskSpec.scheduledExecutionTime(Instant.ofEpochMilli(etaMillis));
    }

    tx.enqueue(settings.getQueueName(), workerTaskSpec.build());
  }

  @Override
  public void completeShard(@NonNull final ShardedJobRunId jobId, @NonNull final IncrementalTaskId taskId) {
    log.info("Polling task states for job " + jobId);
    PipelineService pipelineService = pipelineServiceProvider.get();

    //below seems to FAIL bc of transaction connection - why!?!?
    ShardedJobStateImpl<?> jobState = RetryExecutor.call(FOREVER_RETRYER, () -> {
      PipelineBackendTransaction txn = PipelineBackendTransaction.newInstance(getDatastore(), taskQueue);
      try {
        ShardedJobStateImpl<?> jobState1 = lookupJobState(txn, jobId);
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
        txn.put(jobState1.toEntity(txn));
        txn.commit();
        return jobState1;
      } finally {
        txn.rollbackIfActive();
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



  private <T extends IncrementalTask> IncrementalTaskStatus validateTaskState(IncrementalTaskState<T> taskState,
                                                                              int sequenceNumber, ShardedJobStateImpl<T> jobState) {
    if (taskState == null) {
      return IncrementalTaskStatus.TASK_GONE;
    }
    if (!taskState.getStatus().isActive()) {
      return IncrementalTaskStatus.TASK_NO_LONGER_ACTIVE;
    }
    if (!jobState.getStatus().isActive()) {
      return IncrementalTaskStatus.JOB_NO_LONGER_ACTIVE;
    }

    //sequenceNumber cases
    if (sequenceNumber == taskState.getSequenceNumber()) {
      if (taskState.getLockInfo().isLocked()) {
        return IncrementalTaskStatus.LOCK_HELD_BY_OTHER_EXECUTION;
      } else {
        return IncrementalTaskStatus.TASK_OK;
      }
    } else if (taskState.getSequenceNumber() > sequenceNumber) {
      return IncrementalTaskStatus.TASK_ALREADY_COMPLETED;
    } else {
      return IncrementalTaskStatus.TASK_STATE_FROM_PAST;
    }
  }

  /**
   * Handle a locked slice case.
   */
  private <T extends IncrementalTask> void handleLockHeld(PipelineBackendTransaction tx, IncrementalTaskId taskId, ShardedJobStateImpl<T> jobState,
                                                          IncrementalTaskState<T> taskState) {
    long currentTime = System.currentTimeMillis();
    int sliceTimeoutMillis = jobState.getSettings().getSliceTimeoutMillis();
    long lockExpiration = taskState.getLockInfo().lockedSince() + sliceTimeoutMillis;

    //NOTE: always 'false' now; requests that complete properly SHOULD release their locks..
    boolean wasRequestCompleted = false; // wasRequestCompleted(taskState.getLockInfo().getRequestId());

    if (lockExpiration > currentTime && !wasRequestCompleted) {
      // if lock was not expired AND not abandon reschedule in 1 minute.
      long eta = Math.min(lockExpiration, currentTime + getLockCheckTaskDelay().toMillis());

      scheduleWorkerTask(jobState.getSettings(), taskState, eta, tx);
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

  private <T extends IncrementalTask> boolean lockShard(PipelineBackendTransaction tx,
                                                        IncrementalTaskState<T> taskState, String operationId) {
    boolean locked = false;

    taskState.getLockInfo().lock(operationId);
    Entity entity = taskState.toEntity(tx);
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
  public void runTask(final ShardedJobRunId jobId, final IncrementalTaskId taskId, final int sequenceNumber, String operationId) {
    //acquire lock (allows this process to START potentially long-running work of task itself)

    RetryExecutor.<Void>call(FOREVER_RETRYER, () -> {
      PipelineBackendTransaction lockAcquisition = PipelineBackendTransaction.newInstance(getDatastore(), taskQueue);
      try {
        final ShardedJobStateImpl<? extends IncrementalTask> jobState = lookupJobState(lockAcquisition, jobId);

        if (jobState == null) {
          log.info(taskId + ": Job is gone, ignoring runTask call.");
          return null;
        }

        //taskState represents attempt of executing a slice of a shard of a sharded job
        IncrementalTaskState taskState = lookupTaskState(lockAcquisition, taskId);
        IncrementalTaskStatus validationResult = validateTaskState(taskState, sequenceNumber, jobState);
        if (!validationResult.passed()) {
          switch (validationResult) {
            case TASK_GONE:
              log.info(taskId + ": Task is gone, ignoring runTask call.");
              lockAcquisition.commit();
              //throw, so we re-try a few moments later and see if taskState has been updated
              throw new IllegalStateException("Task is gone");

            case TASK_NO_LONGER_ACTIVE:
              log.info(taskId + ": Task is no longer active, ignoring runTask call.");
              lockAcquisition.commit();
              return null; //we're done here

            case JOB_NO_LONGER_ACTIVE:
              taskState.setStatus(new Status(StatusCode.ABORTED));
              log.info(taskId + ": Job no longer active: " + jobState + ", aborting task.");

              //TODO: has side-effects (enqueuing things)
              updateTask(lockAcquisition, jobState, taskState, null, false);
              lockAcquisition.commit();
              return null; // we're done here

            case TASK_STATE_FROM_PAST:
              log.warning(taskId + " sequenceNumber=" + sequenceNumber + " : Task state is from the past: " + taskState);
              // presumably we are reading an old state
              // task to execute sequenceNumber was enqueued, but state in datastore does not yet reflect it
              // this can happen now because we no longer have transactions across Cloud Tasks + Datastore
              // we should not proceed with this state, throw an IllegalStateException to force retry
              lockAcquisition.commit();
              //throw, so we re-try a few moments later and see if taskState has been updated
              throw new IllegalStateException("Task state is from the past");

            case TASK_ALREADY_COMPLETED:
              log.info(taskId + ": Task sequence number " + sequenceNumber + " already completed: "
                + taskState);
              lockAcquisition.commit();
              return null; // we're done here

            case LOCK_HELD_BY_OTHER_EXECUTION:
              //TODO: has side-effects (enqueuing things); essentially underneath makes decisions that better to wait by enqueuing fresh Cloud Task
              // than waiting in this thread ... refactor??
              handleLockHeld(lockAcquisition, taskId, jobState, taskState);
              log.info(taskId + ": Lock is held by another execution, retrying.");
              lockAcquisition.commit();
              return null; // we're done here; task scheduled by 'handleLockHeld' will pick-up execution
            default:
              log.severe("Unknown validation error: " + validationResult);
              lockAcquisition.rollback();
              throw new IllegalStateException("Validation error: " + validationResult);
              //throw, so re-try moments later with fresh state
          }
          //should be unreachable
        }

        //OK, good to lock and run
        if (lockShard(lockAcquisition, taskState, operationId)) {
          // committing here, which forces acquisition of lock ...
          lockAcquisition.commit();

          // actual task execution
          runAndUpdateTask(jobState.getShardedJobId(), taskId, sequenceNumber, jobState, taskState);
        } else {
          log.warning("Failed to acquire the lock, Will reschedule task for: " + taskState.getJobId()
            + " on slice " + taskState.getSequenceNumber());
          long eta = System.currentTimeMillis() + new Random().nextInt(5000) + 5000;
          scheduleWorkerTask(jobState.getSettings(), taskState, eta, lockAcquisition);
          lockAcquisition.commit();
        }
      } finally {
        lockAcquisition.rollbackIfActive();
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
        PipelineBackendTransaction postRunUpdate = PipelineBackendTransaction.newInstance(getDatastore(), taskQueue);
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
          postRunUpdate.rollbackIfActive();
        }
        return null;
      });

    }

  }

  private <T extends IncrementalTask> ShardRetryState<T> handleSliceFailure(
    PipelineBackendTransaction tx,
    ShardedJobStateImpl<T> jobState,
    IncrementalTaskState<T> taskState,
    RuntimeException ex,
    boolean failedDueToAbandonedLock) {

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
      PipelineBackendTransaction tx,
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

  private <T extends IncrementalTask> void handleJobFailure(PipelineBackendTransaction tx, IncrementalTaskState<T> taskState, Exception ex) {
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
    final PipelineBackendTransaction tx,
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
                                      ShardRetryState<T> shardRetryState, PipelineBackendTransaction tx) {
            Entity taskStateEntity = taskState.toEntity(tx);
            if (shardRetryState == null) {
              tx.put(taskStateEntity);
            } else {
              Entity retryStateEntity = shardRetryState.toEntity(tx);
              tx.put(taskStateEntity, retryStateEntity);
            }
          }

          private void scheduleTask(ShardedJobStateImpl<T> jobState,
                                    IncrementalTaskState<T> taskState, PipelineBackendTransaction tx) {
            if (taskState.getStatus().isActive()) {
              // this used to be transactional, but no longer is with new libraries; so enqueue with a little delay, in hope
              // that the transaction will be committed by the time the task is executed
              scheduleWorkerTask(jobState.getSettings(), taskState, System.currentTimeMillis() + getWorkerTaskDelay().toMillis(), tx);
            } else {
              scheduleControllerTask(jobState.getShardedJobId(), taskState.getTaskId(), jobState.getSettings(), tx);
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

    // NOTE: could probably parallelize this, but causes tests to fail due to threading issues atm
    IntStream.range(0, initialTasks.size()).forEach(taskNumber -> {
      T initialTask = initialTasks.get(taskNumber);
      IncrementalTaskId taskId = IncrementalTaskId.of(jobId, taskNumber);
      //each distinct entity group; see: com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskStateTest.hasNoParent

      // TODO(user): shardId (as known to WorkerShardTask) and taskId happen to be the same
      // number, just because they are created in the same order and happen to use their ordinal.
      // We should have way to inject the "shard-id" to the task.
      RetryExecutor.call(FOREVER_RETRYER, () -> {
        PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(datastore, taskQueue);
        try {
          IncrementalTaskState<T> taskState = IncrementalTaskState.create(taskId, jobId, startTime, initialTask);
          ShardRetryState < T > retryState = ShardRetryState.createFor(taskState);

          // since we should be in case where taskState does not exist, use add() to throw error if it does
          tx.add(taskState.toEntity(tx), retryState.toEntity(tx));
          scheduleWorkerTask(settings, taskState, null, tx);
          tx.commit();
        } catch (DatastoreException e) {
          if (isEntityAlreadyExists(e)) {
            // shouldn't be possible unless we're entering loop to create initial tasks AGAIN, which could only happen on a 2nd StartJob with same
            // ID, right??

            // write should be transactional, so existence of either taskState or retryState implies existence of both
            log.warning(jobId + ": Initial task already exists: " + taskId );
          } else {
            throw e;
          }
        } finally {
          tx.rollbackIfActive();
        }
        return null;
      });
    });
  }

  /**
   * determine semantics of the exception
   *
   * https://cloud.google.com/datastore/docs/concepts/errors#Error_Codes
   *
   * @param e
   * @return
   */
  boolean isEntityAlreadyExists(DatastoreException e) {
    return Objects.equals("ALREADY_EXISTS", e.getReason());
  }

  @SneakyThrows
  private <T extends IncrementalTask> void writeInitialJobState(Datastore datastore, ShardedJobStateImpl<T> jobState) {
    ShardedJobRunId jobId = jobState.getShardedJobId();
    RetryExecutor.call( FOREVER_RETRYER, ()-> {
        PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(datastore, taskQueue);
        try {
          tx.add(jobState.toEntity(tx));
          log.info(jobId + ": wrote initial job state");
          tx.commit();
        } catch (DatastoreException e) {
          if (isEntityAlreadyExists(e)) {
            log.info(jobId + ": Initial job state already exists, left unchanged.");
          } else {
            throw e;
          }
        } finally {
          tx.rollbackIfActive();
        }
      return null;
    });
  }

  public <T extends IncrementalTask> void startJob(final ShardedJobRunId jobId, List<? extends T> initialTasks,
                                                   ShardedJobController<T> controller, ShardedJobSettings settings) {
    Instant startTime = Instant.now();
    Datastore datastore = getDatastore();
    Preconditions.checkArgument(!Iterables.any(initialTasks, Predicates.isNull()),
      "Task list must not contain null values");


    if (settings.getModule() == null) {
      settings =  settings.toBuilder().module(pipelineServiceProvider.get().getDefaultWorkerService()).build();
    }

    ShardedJobStateImpl<T> jobState =
      ShardedJobStateImpl.create(jobId, controller, settings, initialTasks.size(), startTime);
    if (initialTasks.isEmpty()) {
      log.info(jobId + ": No tasks, immediately complete: " + controller);
      jobState.setStatus(new Status(DONE));
      PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(datastore, taskQueue);
      try {
        tx.put(jobState.toEntity(tx));
        tx.commit();
      } finally {
        tx.rollbackIfActive();
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
    PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(datastore, taskQueue);
    try {
      ShardedJobState state = Optional.ofNullable(datastore.get(ShardedJobStateImpl.ShardedJobSerializer.makeKey(datastore, jobId)))
        .map(in -> ShardedJobStateImpl.ShardedJobSerializer.fromEntity(tx, in, true))
        .orElse(null);
      tx.commit();
      return state;
    } finally {
      tx.rollbackIfActive();
    }
  }

  private void changeJobStatus(PipelineBackendTransaction txn, ShardedJobRunId jobId, Status status) {
    log.info(jobId + ": Changing job status to " + status);

    ShardedJobStateImpl<?> jobState = lookupJobState(txn, jobId);
    if (jobState == null || !jobState.getStatus().isActive()) {
      log.info(jobId + ": Job not active, can't change its status: " + jobState);
      return;
    }
    jobState.setStatus(status);
    txn.put(jobState.toEntity(txn));
  }

  public void abortJob(ShardedJobRunId jobId) {
    RetryExecutor.call(FOREVER_RETRYER, () -> {
      PipelineBackendTransaction pipelineBackendTransaction = PipelineBackendTransaction.newInstance(getDatastore(), taskQueue);
      try {
        changeJobStatus(pipelineBackendTransaction, jobId, new Status(ABORTED));
        pipelineBackendTransaction.commit();
      } finally {
        pipelineBackendTransaction.rollbackIfActive();
      }
      return null;
    });
  }


  public boolean cleanupJob(ShardedJobRunId jobId) {
    PipelineBackendTransaction pipelineBackendTransaction = PipelineBackendTransaction.newInstance(getDatastore(), taskQueue);
    ShardedJobStateImpl<?> jobState = lookupJobState(pipelineBackendTransaction, jobId);
    pipelineBackendTransaction.commit(); // just to close the tnx
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

    RetryExecutor.call(FOREVER_RETRYER, callable(() -> {
      PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(datastore, taskQueue);
      try {
        tx.delete(jobKey);
        tx.commit();
      } finally {
        tx.rollbackIfActive();
      }
    } ));
    return true;
  }

  /**
   * Notifies the caller that the job has completed.
   */
  public void enqueueCallbackTask( final ShufflerParams shufflerParams,
                      final String url,
                      final String taskName) {


    String hostname = getWorkerServiceHostName(ShardedJobSettings.builder()
      .module(shufflerParams.getCallbackService())
      .version(shufflerParams.getCallbackVersion()).build());

    String separator = shufflerParams.getCallbackPath().contains("?") ? "&" : "?";


    PipelineTaskQueue.TaskSpec.TaskSpecBuilder taskSpecBuilder = PipelineTaskQueue.TaskSpec.builder()
      .name(taskName)
      .method(PipelineTaskQueue.TaskSpec.Method.GET)
      .host(hostname)
      .callbackPath(shufflerParams.getCallbackPath() + separator + url);

    taskQueue.enqueue(shufflerParams.getCallbackQueue(), taskSpecBuilder.build());
  }


  private String getWorkerServiceHostName(ShardedJobSettings settings) {
    String version = Optional.ofNullable(settings.getVersion()).orElseGet(() ->
      appEngineServicesService.getDefaultVersion(settings.getModule())
    );

    return appEngineServicesService.getWorkerServiceHostName(settings.getModule(), version);
  }

}
