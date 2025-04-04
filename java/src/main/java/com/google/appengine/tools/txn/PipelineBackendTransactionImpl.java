package com.google.appengine.tools.txn;


import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.cloud.datastore.*;
import com.google.cloud.datastore.models.ExplainOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.java.Log;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Transaction wrapper class that aims to mimic cross-services transactions. In this case datastore-cloud tasks.
 */
@Log
public class PipelineBackendTransactionImpl implements PipelineBackendTransaction {

  private static Duration ENQUEUE_DELAY_FOR_SAFER_ROLLBACK = Duration.ofSeconds(30);

  private static final boolean isCloud = System.getProperty("GAE_VERSION", System.getenv("GAE_VERSION")) != null;

  static {
    if (!isCloud) {
      // presumably never set for tests - so this is equivalent to isTesting
      ENQUEUE_DELAY_FOR_SAFER_ROLLBACK = Duration.ZERO;
      log.warning("ENQUEUE_DELAY_FOR_SAFER_ROLLBACK set to 0, this is not a production environment");
    }
  }

  // lazily open in getDsTransaction
  private Transaction dsTransaction;

  private Stopwatch stopwatch;

  @Getter
  final Datastore datastore;

  final PipelineTaskQueue taskQueue;

  public PipelineBackendTransactionImpl(@NonNull Datastore datastore, @NonNull PipelineTaskQueue taskQueue) {
    this.datastore = datastore;
    this.taskQueue = taskQueue;
  }

  @Getter(AccessLevel.PACKAGE)
  @VisibleForTesting
  private final Multimap<String, PipelineTaskQueue.TaskSpec> pendingTaskSpecsByQueue = LinkedHashMultimap.create();

  private final Collection<PipelineTaskQueue.TaskReference> taskReferences = new ArrayList<>();

  /** Transaction interface delegate **/

  public Response commit() {
    //noinspection unchecked
    try {
      Response dsResponse = getDsTransaction().commit();
      // log.info("commit transaction for " + Arrays.stream(Thread.currentThread().getStackTrace()).toList().get(2));
      // we see more Datastore errors (contention / ) than cloud tasks enqueue errors (barely none)
      // let's only commit if the datastore txn went through
      taskReferences.addAll(this.commitTasks());
      return dsResponse;
    } catch (Throwable t) {
      rollbackTasks();
      throw t;
    } finally {
      log.log(Level.FINE, String.format("Transaction commit %s- opened for %s", dsTransaction.getTransactionId().toStringUtf8(), stopwatch.elapsed()));
    }
  }

  public void rollback() {
    try {
      rollbackAllServices();
    } finally {
      log.log(Level.WARNING, String.format("Transaction rollback - opened for %s", stopwatch.elapsed()));
    }
  }

  public boolean rollbackIfActive() {
    boolean shouldLog = false;
    try {
      if (getDsTransaction().isActive()) {
        shouldLog = true;
        this.rollbackAllServices();
      }
    } catch (RuntimeException e) {
      log.log(Level.WARNING, "Rollback of transaction failed: ", e);
    } finally {
      if (shouldLog) {
        log.log(Level.WARNING, String.format("Transaction rollback bc still active - opened for %s", stopwatch.elapsed()));
      }
    }
    return shouldLog;
  }

  @Override
  public Entity get(Key var1) {
    return getDsTransaction().get(var1);
  }

  @Override
  public Iterator<Entity> get(Key... var1) {
    return getDsTransaction().get(var1);
  }

  @Override
  public List<Entity> fetch(Key... var1) {
    return getDsTransaction().fetch(var1);
  }

  @Override
  public <T> QueryResults<T> run(Query<T> var1) {
    return getDsTransaction().run(var1);
  }

  @Override
  public void delete(Key... var1) {
    getDsTransaction().delete(var1);
  }

  @Override
  public Entity put(FullEntity<?> var1) {
    return getDsTransaction().put(var1);
  }

  @Override
  public List<Entity> put(FullEntity<?>... var1) {
    return getDsTransaction().put(var1);
  }

  @Override
  public boolean isActive() {
    return getDsTransaction().isActive();
  }

  @Override
  public <T> QueryResults<T> run(Query<T> query, ExplainOptions explainOptions) {
    return getDsTransaction().run(query, explainOptions);
  }

  @Override
  public void addWithDeferredIdAllocation(FullEntity<?>... fullEntities) {
    getDsTransaction().addWithDeferredIdAllocation(fullEntities);
  }

  @Override
  public Entity add(FullEntity<?> fullEntity) {
    return getDsTransaction().add(fullEntity);
  }

  @Override
  public List<Entity> add(FullEntity<?>... fullEntities) {
    return getDsTransaction().add(fullEntities);
  }

  @Override
  public void update(Entity... entities) {
    getDsTransaction().update(entities);
  }

  @Override
  public void putWithDeferredIdAllocation(FullEntity<?>... fullEntities) {
    getDsTransaction().putWithDeferredIdAllocation(fullEntities);
  }

  @Override
  public ByteString getTransactionId() {
    return getDsTransaction().getTransactionId();
  }

  /** Transaction interface delegate end **/

  @Override
  public void enqueue(Collection<PipelineTask> pipelineTasks) {
    pendingTaskSpecsByQueue.putAll(taskQueue.asTaskSpecs(pipelineTasks));
  }

  @Override
  public void enqueue(String queueName, Collection<PipelineTaskQueue.TaskSpec> taskSpecs) {
    pendingTaskSpecsByQueue.putAll(queueName, taskSpecs);
  }

  /** PipelineTaskQueue interface delegate **/

  private synchronized Transaction getDsTransaction() {
    if (this.dsTransaction == null) {
      synchronized (this) {
        if (this.dsTransaction == null) {
          this.dsTransaction = datastore.newTransaction();
          this.stopwatch = Stopwatch.createStarted();
        }
      }
    }
    return this.dsTransaction;
  }

  private Collection<PipelineTaskQueue.TaskReference> commitTasks() {
    if (!pendingTaskSpecsByQueue.isEmpty()) {
      //noinspection unchecked
      // pipeline specs
      List<PipelineTaskQueue.TaskReference> taskReferences = new ArrayList<>();
      pendingTaskSpecsByQueue.asMap()
        .forEach((queue, tasks) -> {
          // PoC: we can deal with the delay here prior to commit
          Instant fixedNow = Instant.now();
          Collection<PipelineTaskQueue.TaskSpec> delayedTasks = tasks.stream()
            .map(task -> task.withScheduledExecutionTime(Optional.ofNullable(task.getScheduledExecutionTime()).orElse(fixedNow).plus(ENQUEUE_DELAY_FOR_SAFER_ROLLBACK)))
            .collect(Collectors.toSet());

          if (delayedTasks.size() != tasks.size()) {
            HashSet<PipelineTaskQueue.TaskSpec> distinctTasks = new HashSet<>(tasks);
            List<PipelineTaskQueue.TaskSpec> duplicatedTasks = tasks.stream()
              .filter(task -> !distinctTasks.add(task))
              .collect(Collectors.toList());
            String message = String.format("Some identical pipeline tasks were enqueued. Duplicates are %s", duplicatedTasks.stream().map(Object::toString).collect(Collectors.joining(", ")));
            if (isCloud) {
              log.log(Level.WARNING, message);
            } else {
              throw new IllegalStateException(String.format("Some identical pipeline tasks were enqueued. Duplicates are %s", duplicatedTasks.stream().map(Object::toString).collect(Collectors.joining(", "))));
            }
          }

          taskReferences.addAll(taskQueue.enqueue(queue, delayedTasks));
        });
      pendingTaskSpecsByQueue.clear();
      return taskReferences;
    } else {
      return Collections.emptyList();
    }
  }

  private void rollbackTasks() {
    // two cases here that should be mutually exclusive, but deal together for simplicity:
    // 1. if it was never enqueued, just clear the tasks
    if (!pendingTaskSpecsByQueue.isEmpty()) {
      log.log(Level.WARNING, String.format("Rollback never enqueued %d tasks", pendingTaskSpecsByQueue.asMap().values().stream().map(Collection::size).reduce(Integer::sum).orElse(-1)));
      pendingTaskSpecsByQueue.clear();
    }
    // 2. if anything was enqueued, delete it,
    if (!taskReferences.isEmpty()) {
      log.log(Level.WARNING, String.format("Rollback already enqueued %d tasks: %s", taskReferences.size(),
              taskReferences.stream().map(PipelineTaskQueue.TaskReference::getTaskName).collect(Collectors.joining(","))));
      taskQueue.deleteTasks(taskReferences);
      taskReferences.clear();
    }
  }

  private void rollbackAllServices() {
    getDsTransaction().rollback();
    rollbackTasks();
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      if (this.getDsTransaction().isActive()) {
        // shouldn't happen, unless opening tnx just for read, just is kind of absurd in a strong consistency model
        log.log(Level.WARNING, String.format("Finalizing PipelineBackendTransactionImpl transaction open for %s", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
      }
    } finally {
      super.finalize();
    }
  }
}
