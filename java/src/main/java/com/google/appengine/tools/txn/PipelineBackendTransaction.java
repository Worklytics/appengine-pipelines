package com.google.appengine.tools.txn;

import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Transaction;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Collections;

public interface PipelineBackendTransaction extends Transaction {

  /**
   * Rolls back the transaction if still active (most likely was never closed by a commit that failed)
   */
  void rollbackIfActive();

  /* Interface for queues needs to be a little different as can't return TaskReferences, but voids */

  default void enqueue(PipelineTask pipelineTask) {
    enqueue(Collections.singleton(pipelineTask));
  }

  void enqueue(Collection<PipelineTask> pipelineTasks);

  default void enqueue(String queueName, PipelineTaskQueue.TaskSpec taskSpec) {
    enqueue(queueName, Collections.singleton(taskSpec));
  }

  void enqueue(String queueName, Collection<PipelineTaskQueue.TaskSpec> taskSpecs);

  static PipelineBackendTransaction newInstance(Datastore datastore, PipelineTaskQueue taskQueue) {
    return new PipelineBackendTransactionImpl(datastore, taskQueue);
  }

  /**
   * implementation with no-op task handling. Should only be used in specific scenarios where only DS transactions
   * are needed.
   * Should be used mostly in test only scenarios. Marked as deprecated to raise awareness
   * {@link com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline.DeleteShardsInfos} uses it
   * {@link com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline.FinalizeShardsInfos} uses it
   * @param datastore
   * @deprecated
   * @return
   */
  @Deprecated
  static PipelineBackendTransaction newInstance(Datastore datastore) {
    return new PipelineBackendTransactionImpl(datastore, new PipelineTaskQueue() {
      @Override
      public TaskReference enqueue(String queueName, TaskSpec taskSpec) {
        throw new UnsupportedOperationException();
      }

      @Override
      public TaskReference enqueue(PipelineTask pipelineTask) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Collection<TaskReference> enqueue(String queueName, Collection<TaskSpec> taskSpecs) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Collection<TaskReference> enqueue(Collection<PipelineTask> pipelineTasks) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void deleteTasks(Collection<TaskReference> taskReferences) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Multimap<String, TaskSpec> asTaskSpecs(Collection<PipelineTask> pipelineTasks) {
        throw new UnsupportedOperationException();
      }
    });
  }
}
