package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import com.google.appengine.tools.mapreduce.RetryExecutor;
import com.google.appengine.tools.mapreduce.impl.shardedjob.*;
import com.google.appengine.tools.mapreduce.impl.util.DatastoreSerializationUtil;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.txn.PipelineBackendTransaction;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import lombok.RequiredArgsConstructor;

import java.io.Serial;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.Executors.callable;

/**
 * A pipeline job to delete persistent data for a range of shards of a sharded job.
 */
@RequiredArgsConstructor
public class DeleteShardsInfos extends Job0<Void> {

  @Serial
  private static final long serialVersionUID = -4342214189527672009L;

  private final DatastoreOptions datastoreOptions;
  private final ShardedJobRunId jobId;
  private final int start;
  private final int end;


  private static void addParentKeyToList(PipelineBackendTransaction tx, List<Key> list, Key parent) {
    for (Key child : DatastoreSerializationUtil.getShardedValueKeysFor(tx, parent, null)) {
      list.add(child);
    }
    list.add(parent);
  }

  @Override
  public Value<Void> run() {
    Datastore datastore = datastoreOptions.toBuilder().build().getService();
    final List<Key> toDelete = new ArrayList<>((end - start) * 2);

    PipelineBackendTransaction tx = PipelineBackendTransaction.newInstance(datastore);
    for (int i = start; i < end; i++) {
      IncrementalTaskId taskId = IncrementalTaskId.of(jobId, i);
      addParentKeyToList(tx, toDelete, IncrementalTaskState.makeKey(datastore, taskId));
      addParentKeyToList(tx, toDelete, ShardRetryState.Serializer.makeKey(datastore, taskId));
    }
    RetryExecutor.call(
      ShardedJobRunner.FOREVER_RETRYER,
      callable(() -> tx.delete(toDelete.toArray(new Key[toDelete.size()]))));

    tx.commit();

    return null;
  }

  @Override
  public String getJobDisplayName() {
    return "DeleteShardsInfos: " + jobId + "[" + start + "-" + end + "]";
  }
}
