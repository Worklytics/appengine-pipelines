package com.google.appengine.tools.txn;


import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.cloud.datastore.Transaction;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

/**
 * Transaction wrapper class that aims to mimic cross-services transactions. In this case datastore-cloud tasks.
 */
@Log
@RequiredArgsConstructor(staticName = "of")
public class TxnWrapper {

  @NonNull
  @Getter // should only be accessed when adding stuff to the txn
  Transaction dsTransaction;

  final PipelineTaskQueue taskQueue;

  private final Multimap<String, PipelineTaskQueue.TaskSpec> tasksByQueue = LinkedHashMultimap.create();

  private final Collection<PipelineTaskQueue.TaskReference> taskReferences = new ArrayList<>();

  public void commit() {
    //noinspection unchecked
    try {
      taskReferences.addAll(this.commitTasks());
      // returning void for simplicity, we never do anything with the response
      dsTransaction.commit();
    } catch (Throwable t) {
      rollbackTasks();
      throw t;
    }
  }

  public void addTask(String queue, PipelineTaskQueue.TaskSpec task) {
    tasksByQueue.put(queue, task);
  }

  public void rollback() {
    dsTransaction.rollback();
    // two cases here that should be mutually exclusive, but deal together for simplicity:
    // 1. if it was never enqueued, just clear the tasks
    tasksByQueue.clear();
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

  private Collection<PipelineTaskQueue.TaskReference> commitTasks() {
    if (!tasksByQueue.isEmpty()) {
      Preconditions.checkState(dsTransaction.isActive());
      Preconditions.checkNotNull(taskQueue, "Missing PipelineTaskQueue: can't enqueue");
      //noinspection unchecked
      List<PipelineTaskQueue.TaskReference> taskReferences = new ArrayList<>();
      tasksByQueue.asMap()
        .forEach((queue, tasks) -> taskReferences.addAll(taskQueue.enqueue(queue, tasks)));
      // all commited, clean tasks
      tasksByQueue.clear();
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

}
