package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import static java.util.concurrent.Executors.callable;

import com.google.appengine.tools.mapreduce.RetryExecutor;
import com.google.appengine.tools.mapreduce.impl.shardedjob.*;
import com.google.appengine.tools.mapreduce.impl.util.DatastoreSerializationUtil;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;
import com.google.cloud.datastore.*;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
/**
 * A pipeline job for finalizing the shards information and cleaning up unnecessary state.
 */
@RequiredArgsConstructor
public class FinalizeShardsInfos extends Job0<Void> {

  private static final long serialVersionUID = 1L;

  private final DatastoreOptions datastoreOptions;
  private final ShardedJobRunId jobId;
  private final Status status;
  private final int start;
  private final int end;

  @Override
  public Value<Void> run() {
    Datastore datastore = datastoreOptions.getService();

    RetryExecutor.call(
      ShardedJobRunner.FOREVER_RETRYER,
      callable(() -> {
        Transaction tx = datastore.newTransaction();

        final List<Key> toFetch = new ArrayList<>(end - start);
        final List<Entity> toUpdate = new ArrayList<>();
        final List<Key> toDelete = new ArrayList<>();
        for (int i = start; i < end; i++) {
          IncrementalTaskId taskId = IncrementalTaskId.of(jobId, i);
          toFetch.add(IncrementalTaskState.Serializer.makeKey(datastore, taskId));
          Key retryStateKey = ShardRetryState.Serializer.makeKey(datastore, taskId);
          toDelete.add(retryStateKey);
          for (Key key : DatastoreSerializationUtil.getShardedValueKeysFor(tx, retryStateKey, null)) {
            toDelete.add(key);
          }
        }

        tx.delete(toDelete.toArray(new Key[toDelete.size()]));

        List<Entity> entities = tx.fetch(toFetch.toArray(new Key[toFetch.size()]));
        for (Entity entity : entities) {

          if (entity != null) {
            IncrementalTaskState<IncrementalTask> taskState =
              IncrementalTaskState.Serializer.fromEntity(tx, entity, true);
            if (taskState.getTask() != null) {
              taskState.getTask().jobCompleted(status);
              toUpdate.add(IncrementalTaskState.Serializer.toEntity(tx, taskState));
            }
          }
        }
        if (!toUpdate.isEmpty()) {
          tx.put(toUpdate.toArray(new Entity[toUpdate.size()]));
        }
        tx.commit();
      }));

    return null;
  }

  @Override
  public String getJobDisplayName() {
    return "FinalizeShardsInfos: " + jobId + "[" + start + "-" + end + "]";
  }
}
