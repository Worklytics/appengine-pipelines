// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.deserializeFromDatastoreProperty;
import static com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.cloud.datastore.*;
import com.google.common.primitives.Ints;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

/**
 * Retry information for a shard.
 *
 *
 * @param <T> type of task
 */
@AllArgsConstructor
@Getter
public final class ShardRetryState<T extends IncrementalTask> {

  private final IncrementalTaskId taskId;
  private final T initialTask;
  private int retryCount;

  @Getter(AccessLevel.PRIVATE)
  @Setter(AccessLevel.PRIVATE)
  private int initialTaskShards;

  public int incrementAndGet() {
    return ++retryCount;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + taskId + ", " + retryCount + ", " + initialTask + ")";
  }

  static <T extends IncrementalTask> ShardRetryState<T> createFor(
      IncrementalTaskState<T> taskState) {
    return new ShardRetryState<>(taskState.getTaskId(), taskState.getTask(), 0, 0);
  }

  /**
   * Utility class to serialize/deserialize ShardRetryState.
   * ShardRetryState should be using the same transactions as IncrementalTaskState
   */
  public static class Serializer {
    private static final String ENTITY_KIND = "MR-ShardRetryState";
    private static final String INITIAL_TASK_PROPERTY = "initialTask";
    private static final String RETRY_COUNT_PROPERTY = "retryCount";
    private static final String TASK_ID_PROPERTY = "taskId";

    public static Key makeKey(Datastore datastore, IncrementalTaskId taskId) {

      Key parent = IncrementalTaskState.Serializer.makeKey(datastore, taskId);
      return Key.newBuilder(parent, ENTITY_KIND, 1L).build();
    }

    static Entity toEntity(Transaction tx, ShardRetryState<?> in) {
      Entity.Builder shardInfo = Entity.newBuilder(makeKey(tx.getDatastore(), in.getTaskId()));
      int initialTaskShards = serializeToDatastoreProperty(tx, shardInfo, INITIAL_TASK_PROPERTY, in.initialTask, Optional.of(in.initialTaskShards));
      in.setInitialTaskShards(initialTaskShards);
      shardInfo.set(RETRY_COUNT_PROPERTY, LongValue.newBuilder(in.retryCount).setExcludeFromIndexes(true).build());
      shardInfo.set(TASK_ID_PROPERTY, in.taskId.asEncodedString());
      return shardInfo.build();
    }

    static <T extends IncrementalTask> ShardRetryState<T> fromEntity(Transaction tx, Entity in) {
      T initialTask = deserializeFromDatastoreProperty(tx, in, INITIAL_TASK_PROPERTY);
      int retryCount = Ints.checkedCast(in.getLong(RETRY_COUNT_PROPERTY));
      IncrementalTaskId taskId = IncrementalTaskId.parse(in.getString(TASK_ID_PROPERTY));
      return new ShardRetryState<>(taskId, initialTask, retryCount, SerializationUtil.shardsUsedToStore(in, INITIAL_TASK_PROPERTY));
    }
  }
}
