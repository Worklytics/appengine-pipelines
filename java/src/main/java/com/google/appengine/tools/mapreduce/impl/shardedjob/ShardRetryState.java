// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.DatastoreSerializationUtil.deserializeFromDatastoreProperty;
import static com.google.appengine.tools.mapreduce.impl.util.DatastoreSerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.impl.util.DatastoreSerializationUtil;
import com.google.appengine.tools.pipeline.impl.model.ExpiringDatastoreEntity;
import com.google.appengine.tools.txn.PipelineBackendTransaction;
import com.google.cloud.datastore.*;
import com.google.common.primitives.Ints;
import lombok.*;

import java.time.Instant;
import java.util.Optional;

/**
 * Retry information for a shard.
 *
 *
 * @param <T> type of task
 */
@Builder(access=AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public final class ShardRetryState<T extends IncrementalTask> implements ExpiringDatastoreEntity {

  private static final String DATASTORE_KIND = "MR-ShardRetryState";

  private final IncrementalTaskId taskId;
  private final T initialTask;
  private int retryCount;

  // internal tracking of how many shards were used to store the task value, if too large to be single Blob property
  // eg, for a large serialized task, this will be the number of shards used to store the serialized task; not to be confused with other notions of sharding
  @Getter(AccessLevel.PRIVATE)
  @Setter(AccessLevel.PRIVATE)
  private int initialTaskValueShards;

  @Builder.ObtainVia(method = "defaultExpireAt")
  @Getter @Setter
  private Instant expireAt;

  public int incrementAndGet() {
    return ++retryCount;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + taskId + ", " + retryCount + ", " + initialTask + ")";
  }

  /**
   * serialize to datastore entity in context of a transaction
   * @param tx transaction
   * @return datastore entity
   */
  public Entity toEntity(PipelineBackendTransaction tx) {
    Entity.Builder shardInfo = Entity.newBuilder(Serializer.makeKey(tx.getDatastore(), getTaskId()));
    int initialTaskShards = serializeToDatastoreProperty(tx, shardInfo, Serializer.INITIAL_TASK_PROPERTY, initialTask, Optional.of(initialTaskValueShards));
    this.setInitialTaskValueShards(initialTaskShards);
    shardInfo.set(Serializer.RETRY_COUNT_PROPERTY, LongValue.newBuilder(retryCount).setExcludeFromIndexes(true).build());
    shardInfo.set(Serializer.TASK_ID_PROPERTY, taskId.asEncodedString());
    fillExpireAt(shardInfo);
    return shardInfo.build();
  }

  static <T extends IncrementalTask> ShardRetryState<T> createFor(
      IncrementalTaskState<T> taskState) {
    return ShardRetryState.<T>builder()
      .taskId(taskState.getTaskId())
      .initialTask(taskState.getTask())
      .retryCount(0)
      .initialTaskValueShards(0)
      .build();
  }

  /**
   * Utility class to serialize/deserialize ShardRetryState.
   * ShardRetryState should be using the same transactions as IncrementalTaskState
   */
  public static class Serializer {
    private static final String INITIAL_TASK_PROPERTY = "initialTask";
    private static final String RETRY_COUNT_PROPERTY = "retryCount";
    private static final String TASK_ID_PROPERTY = "taskId";

    public static Key makeKey(Datastore datastore, IncrementalTaskId taskId) {

      Key parent = IncrementalTaskState.makeKey(datastore, taskId);
      return Key.newBuilder(parent, DATASTORE_KIND, 1L).build();
    }

    static <T extends IncrementalTask> ShardRetryState<T> fromEntity(PipelineBackendTransaction tx, Entity in) {
      T initialTask = deserializeFromDatastoreProperty(tx, in, INITIAL_TASK_PROPERTY);
      int retryCount = Ints.checkedCast(in.getLong(RETRY_COUNT_PROPERTY));
      IncrementalTaskId taskId = IncrementalTaskId.parse(in.getString(TASK_ID_PROPERTY));

      return ShardRetryState.<T>builder()
        .taskId(taskId)
        .initialTask(initialTask)
        .retryCount(retryCount)
        .initialTaskValueShards(DatastoreSerializationUtil.shardsUsedToStore(in, INITIAL_TASK_PROPERTY))
        .expireAt(ExpiringDatastoreEntity.getExpireAt(in))
        .build();
    }
  }
}
