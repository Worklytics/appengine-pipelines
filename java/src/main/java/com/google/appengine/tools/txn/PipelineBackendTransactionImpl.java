package com.google.appengine.tools.txn;


import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.cloud.datastore.*;
import com.google.cloud.datastore.models.ExplainOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.java.Log;

import java.util.*;
import java.util.logging.Level;

/**
 * Transaction wrapper class that aims to mimic cross-services transactions. In this case datastore-cloud tasks.
 */
@Log
public class PipelineBackendTransactionImpl implements PipelineBackendTransaction {

  @NonNull
  @Getter // should only be accessed when adding stuff to the txn
  Transaction dsTransaction;

  @Getter
  final Datastore datastore;

  final PipelineTaskQueue taskQueue;

  public PipelineBackendTransactionImpl(Datastore datastore, PipelineTaskQueue taskQueue) {
    this.datastore = datastore;
    this.taskQueue = taskQueue;
    // open the transaction
    this.dsTransaction = datastore.newTransaction();
  }

  @Getter(AccessLevel.PACKAGE)
  @VisibleForTesting
  private final Multimap<String, PipelineTaskQueue.TaskSpec> pendingTaskSpecsByQueue = LinkedHashMultimap.create();

  private final Collection<PipelineTaskQueue.TaskReference> taskReferences = new ArrayList<>();

  /** Transaction interface delegate **/

  public Response commit() {
    //noinspection unchecked
    try {
      taskReferences.addAll(this.commitTasks());
      // returning void for simplicity, we never do anything with the response
      return dsTransaction.commit();
    } catch (Throwable t) {
      rollbackTasks();
      throw t;
    }
  }

  public void rollback() {
    dsTransaction.rollback();
    // two cases here that should be mutually exclusive, but deal together for simplicity:
    // 1. if it was never enqueued, just clear the tasks
    pendingTaskSpecsByQueue.clear();
    // 2. if anything was enqueued, delete it,
    rollbackTasks();
  }

  public void rollbackIfActive() {
    try {
      if (dsTransaction.isActive()) {
        this.rollback();
      }
    } catch (RuntimeException e) {
      log.log(Level.WARNING, "Rollback of transaction failed: ", e);
    }
  }

  @Override
  public Entity get(Key var1) {
    return dsTransaction.get(var1);
  }

  @Override
  public Iterator<Entity> get(Key... var1) {
    return dsTransaction.get(var1);
  }

  @Override
  public List<Entity> fetch(Key... var1) {
    return dsTransaction.fetch(var1);
  }

  @Override
  public <T> QueryResults<T> run(Query<T> var1) {
    return dsTransaction.run(var1);
  }

  @Override
  public void delete(Key... var1) {
    dsTransaction.delete(var1);
  }

  @Override
  public Entity put(FullEntity<?> var1) {
    return dsTransaction.put(var1);
  }

  @Override
  public List<Entity> put(FullEntity<?>... var1) {
    return dsTransaction.put(var1);
  }

  @Override
  public boolean isActive() {
    return dsTransaction.isActive();
  }

  @Override
  public <T> QueryResults<T> run(Query<T> query, ExplainOptions explainOptions) {
    return dsTransaction.run(query, explainOptions);
  }

  @Override
  public void addWithDeferredIdAllocation(FullEntity<?>... fullEntities) {
    dsTransaction.addWithDeferredIdAllocation(fullEntities);
  }

  @Override
  public Entity add(FullEntity<?> fullEntity) {
    return dsTransaction.add(fullEntity);
  }

  @Override
  public List<Entity> add(FullEntity<?>... fullEntities) {
    return dsTransaction.add(fullEntities);
  }

  @Override
  public void update(Entity... entities) {
    dsTransaction.update(entities);
  }

  @Override
  public void putWithDeferredIdAllocation(FullEntity<?>... fullEntities) {
    dsTransaction.putWithDeferredIdAllocation(fullEntities);
  }

  @Override
  public ByteString getTransactionId() {
    return dsTransaction.getTransactionId();
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


  private Collection<PipelineTaskQueue.TaskReference> commitTasks() {
    if (!pendingTaskSpecsByQueue.isEmpty()) {
      Preconditions.checkState(dsTransaction.isActive());
      Preconditions.checkNotNull(taskQueue, "Missing PipelineTaskQueue: can't enqueue");
      //noinspection unchecked
      // pipeline specs
      List<PipelineTaskQueue.TaskReference> taskReferences = new ArrayList<>();
      pendingTaskSpecsByQueue.asMap()
        .forEach((queue, tasks) -> {
          tasks.stream().map(task -> task.withScheduledExecutionTime(task.getScheduledExecutionTime().plusSeconds(5))).toList();
          taskReferences.addAll(taskQueue.enqueue(queue, tasks));
        });
      pendingTaskSpecsByQueue.clear();
      return taskReferences;
    } else {
      return Collections.emptyList();
    }
  }

  private void rollbackTasks() {
    if (!taskReferences.isEmpty()) {
      Preconditions.checkNotNull(taskQueue, "Missing PipelineTaskQueue: can't delete tasks");
      taskQueue.deleteTasks(taskReferences);
      taskReferences.clear();
    }
  }


  @Override
  protected void finalize() throws Throwable {
    try {
      if (this.dsTransaction.isActive()) {
        log.log(Level.WARNING, new Throwable(), () -> "Finalizing PipelineBackendTransactionImpl w/o committing the transaction");
      }
    } finally {
      super.finalize();
    }
  }
}
