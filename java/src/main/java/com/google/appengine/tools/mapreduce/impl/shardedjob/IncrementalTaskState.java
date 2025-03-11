// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.DatastoreSerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.pipeline.impl.model.ExpiringDatastoreEntity;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.util.DatastoreSerializationUtil;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import lombok.*;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

/**
 * Information about execution of an {@link IncrementalTask}.
 *
 * really, shardExecutionState ...
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of task
 */
@ToString
@Getter
public class IncrementalTaskState<T extends IncrementalTask> implements ExpiringDatastoreEntity {

  static final String DATASTORE_KIND = "MR-IncrementalTask";

  private final ShardedJobRunId jobId;

  private final IncrementalTaskId taskId;

  private final Integer shardNumber;

  @Setter
  private Instant mostRecentUpdateTime;

  /**
   * incremented each (successful) run of task. eg, count of slices.
   */
  @Setter
  private int sequenceNumber;

  private int retryCount;

  @Setter
  private T task;

  @Setter
  private Status status;

  @ToString.Exclude
  private LockInfo lockInfo;

  // internal tracking of how many shards were used to store the task value, if too large to be single Blob property
  // eg, how many entities task value was split into, if it was too large; not to be confused with other notions of sharding
  @Setter(AccessLevel.PRIVATE)
  @Getter(AccessLevel.PRIVATE)
  private Integer taskValueShards;

  // internal tracking of how many shards were used to store the status value, if too large to be single Blob property
  // eg, how many entities status value was split into, if it was too large; not to be confused with other notions of sharding
  @Setter(AccessLevel.PRIVATE)
  @Getter(AccessLevel.PRIVATE)
  private Integer statusValueShards;

  @Setter
  private Instant expireAt;

  public static Key makeKey(Datastore datastore, IncrementalTaskId taskId) {
    return datastore.newKeyFactory().setKind(DATASTORE_KIND).newKey(taskId.asEncodedString());
  }

  public static class LockInfo {

    private Long startTime;

    @Getter
    private String requestId;

    private LockInfo(Long startTime, String requestId) {
      this.startTime = startTime;
      this.requestId = requestId;
    }

    public boolean isLocked() {
      return startTime != null;
    }

    public long lockedSince() {
      return startTime == null ? -1 : startTime;
    }

    public void lock(String requestId) {
      startTime = System.currentTimeMillis();
      this.requestId = requestId;
    }

    public void unlock() {
      startTime = null;
      requestId = null;
    }

    @Override
    public String toString() {
      return getClass().getName() + "(" + startTime + ", " + requestId + ")";
    }
  }

  /**
   * Returns a new running IncrementalTaskState.
   */
  static <T extends IncrementalTask> IncrementalTaskState<T> create(
          IncrementalTaskId taskId, ShardedJobRunId jobId, Instant createTime, T initialTask) {
    return new IncrementalTaskState<>(taskId, jobId, createTime, new LockInfo(null, null),
        checkNotNull(initialTask), new Status(StatusCode.RUNNING));
  }

  private IncrementalTaskState(@NonNull IncrementalTaskId taskId,
                               @NonNull ShardedJobRunId jobId,
                               Instant mostRecentUpdateTime,
                               LockInfo lockInfo,
                               T task,
                               Status status) {
    this.taskId = taskId;
    this.jobId = jobId;
    this.shardNumber = taskId.getNumber();
    this.mostRecentUpdateTime = mostRecentUpdateTime;
    this.lockInfo = lockInfo;
    this.task = task;
    this.taskValueShards = 0;
    this.status = status;
    this.statusValueShards = 0;
    this.expireAt = defaultExpireAt();
  }

  int incrementAndGetRetryCount() {
    return ++retryCount;
  }

  void clearRetryCount() {
    retryCount = 0;
  }

  private static final String JOB_ID_PROPERTY = "jobId";
  private static final String MOST_RECENT_UPDATE_TIME_PROPERTY = "mostRecentUpdateTime";
  private static final String SEQUENCE_NUMBER_PROPERTY = "sequenceNumber";
  private static final String RETRY_COUNT_PROPERTY = "retryCount";
  private static final String SLICE_START_TIME = "sliceStartTime";
  private static final String SLICE_REQUEST_ID = "sliceRequestId";
  private static final String NEXT_TASK_PROPERTY = "nextTask";
  private static final String STATUS_PROPERTY = "status";

  public Entity toEntity(Transaction tx) {
    Key key = makeKey(tx.getDatastore(), this.getTaskId());
    Entity.Builder taskState = Entity.newBuilder(key);
    taskState.set(JOB_ID_PROPERTY, this.getJobId().asEncodedString());
    taskState.set(MOST_RECENT_UPDATE_TIME_PROPERTY,
      TimestampValue.newBuilder(Timestamp.of(Date.from(this.getMostRecentUpdateTime()))).setExcludeFromIndexes(true).build());

    if (this.getLockInfo() != null) {
      if (this.getLockInfo().startTime != null) {
        taskState.set(SLICE_START_TIME, LongValue.newBuilder(this.getLockInfo().startTime).setExcludeFromIndexes(true).build());
      }
      if (this.getLockInfo().requestId != null) {
        taskState.set(SLICE_REQUEST_ID, StringValue.newBuilder(this.getLockInfo().requestId).setExcludeFromIndexes(true).build());
      }
    }
    taskState.set(SEQUENCE_NUMBER_PROPERTY, this.getSequenceNumber());
    taskState.set(RETRY_COUNT_PROPERTY, this.getRetryCount());

    serializeToDatastoreProperty(tx, taskState, NEXT_TASK_PROPERTY, this.getTask(), Optional.ofNullable(this.taskValueShards));
    serializeToDatastoreProperty(tx, taskState, STATUS_PROPERTY, this.getStatus(), Optional.ofNullable(this.statusValueShards));

    fillExpireAt(taskState);

    return taskState.build();
  }

  /**
   * Utility class to serialize/deserialize IncrementalTaskState.
   */
  public static class Serializer {
    static final String SHARD_INFO_ENTITY_KIND = DATASTORE_KIND + "-ShardInfo";


    public static <T extends IncrementalTask> IncrementalTaskState<T> fromEntity(Transaction tx, Entity in) {
      return fromEntity(tx, in, false);
    }

    public static <T extends IncrementalTask> IncrementalTaskState<T> fromEntity(
      @NonNull Datastore datastore,
      Entity in,
      boolean lenient) {
      Transaction txn = datastore.newTransaction();
      IncrementalTaskState<T> state = fromEntity(txn, in, lenient);
      txn.commit();
      return state;
    }

    public static <T extends IncrementalTask> IncrementalTaskState<T> fromEntity(
        @NonNull Transaction tx,
        Entity in,
        boolean lenient) {
      Preconditions.checkArgument(DATASTORE_KIND.equals(in.getKey().getKind()), "Unexpected kind: %s", in);
      LockInfo lockInfo = null;
      if (in.contains(SLICE_START_TIME)) {
        lockInfo = new LockInfo(in.getLong(SLICE_START_TIME),
          in.getString(SLICE_REQUEST_ID));
      } else {
        lockInfo = new LockInfo(null, null);
      }

      IncrementalTaskState<T> state = new IncrementalTaskState<>(
          IncrementalTaskId.parse(in.getKey().getName()),
          ShardedJobRunId.fromEncodedString(in.getString(JOB_ID_PROPERTY)),
          in.getTimestamp(MOST_RECENT_UPDATE_TIME_PROPERTY).toDate().toInstant(),
          lockInfo,
          in.contains(NEXT_TASK_PROPERTY) ? DatastoreSerializationUtil.deserializeFromDatastoreProperty(tx, in, NEXT_TASK_PROPERTY, lenient) : null,
          DatastoreSerializationUtil.deserializeFromDatastoreProperty(tx, in, STATUS_PROPERTY));
      state.setSequenceNumber(
          Ints.checkedCast(in.getLong(SEQUENCE_NUMBER_PROPERTY)));
      if (in.contains(RETRY_COUNT_PROPERTY)) {
        state.retryCount = Ints.checkedCast(in.getLong(RETRY_COUNT_PROPERTY));
      }

      state.setStatusValueShards(DatastoreSerializationUtil.shardsUsedToStore(in, STATUS_PROPERTY));
      state.setTaskValueShards(DatastoreSerializationUtil.shardsUsedToStore(in, NEXT_TASK_PROPERTY));

      state.setExpireAt(ExpiringDatastoreEntity.getExpireAt(in));

      return state;
    }

    static boolean hasNextTask(Entity in) {
      Preconditions.checkArgument(DATASTORE_KIND.equals(in.getKey().getKind()), "Unexpected kind: %s", in);
      return in.contains(NEXT_TASK_PROPERTY);
    }
  }
}
