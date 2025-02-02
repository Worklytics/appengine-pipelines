// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.apphosting.api.ApiProxy;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.Date;

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
public class IncrementalTaskState<T extends IncrementalTask> {

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

    public void lock(ShardedJobHandler.WorkerTaskExecutionId executionId) {
      startTime = System.currentTimeMillis();
      requestId = executionId.encodeAsString();
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
    this.status = status;
  }

  int incrementAndGetRetryCount() {
    return ++retryCount;
  }

  void clearRetryCount() {
    retryCount = 0;
  }

  /**
   * Utility class to serialize/deserialize IncrementalTaskState.
   */
  public static class Serializer {
    static final String ENTITY_KIND = "MR-IncrementalTask";
    static final String SHARD_INFO_ENTITY_KIND = ENTITY_KIND + "-ShardInfo";

    private static final String JOB_ID_PROPERTY = "jobId";
    private static final String MOST_RECENT_UPDATE_TIME_PROPERTY = "mostRecentUpdateTime";
    private static final String SEQUENCE_NUMBER_PROPERTY = "sequenceNumber";
    private static final String RETRY_COUNT_PROPERTY = "retryCount";
    private static final String SLICE_START_TIME = "sliceStartTime";
    private static final String SLICE_REQUEST_ID = "sliceRequestId";
    private static final String NEXT_TASK_PROPERTY = "nextTask";
    private static final String STATUS_PROPERTY = "status";

    public static Key makeKey(Datastore datastore, IncrementalTaskId taskId) {
      return datastore.newKeyFactory().setKind(ENTITY_KIND).newKey(taskId.asEncodedString());
    }

    public static Entity toEntity(Transaction tx, IncrementalTaskState<?> in) {
      Key key = makeKey(tx.getDatastore(), in.getTaskId());
      Entity.Builder taskState = Entity.newBuilder(key);
      taskState.set(JOB_ID_PROPERTY, in.getJobId().asEncodedString());
      taskState.set(MOST_RECENT_UPDATE_TIME_PROPERTY,
        TimestampValue.newBuilder(Timestamp.of(Date.from(in.getMostRecentUpdateTime()))).setExcludeFromIndexes(true).build());

      if (in.getLockInfo() != null) {
        if (in.getLockInfo().startTime != null) {
          taskState.set(SLICE_START_TIME, LongValue.newBuilder(in.getLockInfo().startTime).setExcludeFromIndexes(true).build());
        }
        if (in.getLockInfo().requestId != null) {
          taskState.set(SLICE_REQUEST_ID, StringValue.newBuilder(in.getLockInfo().requestId).setExcludeFromIndexes(true).build());
        }
      }
      taskState.set(SEQUENCE_NUMBER_PROPERTY, in.getSequenceNumber());
      taskState.set(RETRY_COUNT_PROPERTY, in.getRetryCount());
      serializeToDatastoreProperty(tx, taskState, NEXT_TASK_PROPERTY, in.getTask());
      serializeToDatastoreProperty(tx, taskState, STATUS_PROPERTY, in.getStatus());
      return taskState.build();
    }

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
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKey().getKind()), "Unexpected kind: %s", in);
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
          in.contains(NEXT_TASK_PROPERTY) ? SerializationUtil.deserializeFromDatastoreProperty(tx, in, NEXT_TASK_PROPERTY, lenient) : null,
          SerializationUtil.deserializeFromDatastoreProperty(tx, in, STATUS_PROPERTY));
      state.setSequenceNumber(
          Ints.checkedCast(in.getLong(SEQUENCE_NUMBER_PROPERTY)));
      if (in.contains(RETRY_COUNT_PROPERTY)) {
        state.retryCount = Ints.checkedCast(in.getLong(RETRY_COUNT_PROPERTY));
      }
      return state;
    }

    static boolean hasNextTask(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKey().getKind()), "Unexpected kind: %s", in);
      return in.contains(NEXT_TASK_PROPERTY);
    }
  }
}
