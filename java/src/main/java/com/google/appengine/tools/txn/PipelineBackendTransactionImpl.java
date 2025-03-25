package com.google.appengine.tools.txn;


import com.google.appengine.tools.pipeline.impl.backend.AppEngineStandardGen2;
import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.cloud.datastore.*;
import com.google.cloud.datastore.models.ExplainOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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

/**
 * Transaction wrapper class that aims to mimic cross-services transactions. In this case datastore-cloud tasks.
 */
@Log
public class PipelineBackendTransactionImpl implements PipelineBackendTransaction {

  private Duration ENQUEUE_DELAY_FOR_SAFER_ROLLBACK = Duration.ofSeconds(10);

  // lazily open in getDsTransaction
  private Transaction dsTransaction;

  private Stopwatch stopwatch;

  @Getter
  final Datastore datastore;

  final PipelineTaskQueue taskQueue;

  public PipelineBackendTransactionImpl(@NonNull Datastore datastore, @NonNull PipelineTaskQueue taskQueue) {
    this.datastore = datastore;
    this.taskQueue = taskQueue;
    if (System.getProperty("GAE_VERSION") != null) {
      // presumably never set for tests - so this is equivalent to isTesting
      this.ENQUEUE_DELAY_FOR_SAFER_ROLLBACK = Duration.ZERO;
    }
  }

  @Getter(AccessLevel.PACKAGE)
  @VisibleForTesting
  private final Multimap<String, PipelineTaskQueue.TaskSpec> pendingTaskSpecsByQueue = LinkedHashMultimap.create();

  private final Collection<PipelineTaskQueue.TaskReference> taskReferences = new ArrayList<>();

  /** Transaction interface delegate **/

  public Response commit() {
    //noinspection unchecked
    try {
      log.info("commit transaction for " + Arrays.stream(Thread.currentThread().getStackTrace()).toList().get(2));
      taskReferences.addAll(this.commitTasks());
      // returning void for simplicity, we never do anything with the response
      return getDsTransaction().commit();
    } catch (Throwable t) {
      rollbackTasks();
      throw t;
    } finally {
      log.log(Level.FINE, String.format("Transaction open for %s", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }
  }

  public void rollback() {
    try {
      getDsTransaction().rollback();
      rollbackTasks();
    } finally {
      log.log(Level.FINE, String.format("Transaction open for %s", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }
  }

  public void rollbackIfActive() {
    try {
      if (getDsTransaction().isActive()) {
        log.log(Level.FINE, String.format("Transaction open for %s", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
        this.rollback();
      }
    } catch (RuntimeException e) {
      log.log(Level.WARNING, "Rollback of transaction failed: ", e);
    }
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
          Collection<PipelineTaskQueue.TaskSpec> delayedTasks = tasks.stream()
            .map(task -> task.withScheduledExecutionTime(Optional.ofNullable(task.getScheduledExecutionTime()).orElse(Instant.now()).plus(ENQUEUE_DELAY_FOR_SAFER_ROLLBACK)))
            .toList();
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
    pendingTaskSpecsByQueue.clear();
    // 2. if anything was enqueued, delete it,
    if (!taskReferences.isEmpty()) {
      taskQueue.deleteTasks(taskReferences);
      taskReferences.clear();
    }
  }


  @Override
  protected void finalize() throws Throwable {
    try {
      if (this.getDsTransaction().isActive()) {
        log.log(Level.WARNING, String.format("Finalizing PipelineBackendTransactionImpl transaction open for %s", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
      }
    } finally {
      super.finalize();
    }
  }
}
