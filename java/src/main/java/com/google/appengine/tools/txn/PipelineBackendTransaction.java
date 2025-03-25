package com.google.appengine.tools.txn;

import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.cloud.datastore.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public interface PipelineBackendTransaction {

  void commit();

  void addTask(String queue, PipelineTaskQueue.TaskSpec task);

  void rollback();

  void rollbackIfActive();

  Entity get(Key var1);

  Iterator<Entity> get(Key... var1);

  List<Entity> fetch(Key... var1);

  <T> QueryResults<T> run(Query<T> var1);

  void delete(Key... var1);

  Entity put(FullEntity<?> var1);

  List<Entity> put(FullEntity<?>... var1);

  Datastore getDatastore();

  boolean isActive();

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
      public Collection<TaskReference> enqueue(Transaction txn, Collection<PipelineTask> pipelineTasks) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void deleteTasks(Collection<TaskReference> taskReferences) {
      }
    });
  }
}
