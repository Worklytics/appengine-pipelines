// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.datastore.*;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.apphosting.api.ApiProxy;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import lombok.Getter;
import lombok.NonNull;

/**
 * Information about execution of an {@link IncrementalTask}.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of task
 */
@Getter
public class IncrementalTaskState<T extends IncrementalTask> {

  private final String taskId;
  private final String jobId;
  private long mostRecentUpdateMillis;
  private int sequenceNumber;
  private int retryCount;
  private T task;
  private Status status;
  private LockInfo lockInfo;

  public static class LockInfo {

    private static final String REQUEST_ID = "com.google.appengine.runtime.request_log_id";

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

    public void lock() {
      startTime = System.currentTimeMillis();
      requestId = (String) ApiProxy.getCurrentEnvironment().getAttributes().get(REQUEST_ID);
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
      String taskId, String jobId, long createTime, T initialTask) {
    return new IncrementalTaskState<>(taskId, jobId, createTime, new LockInfo(null, null),
        checkNotNull(initialTask), new Status(StatusCode.RUNNING));
  }

  private IncrementalTaskState(String taskId, String jobId, long mostRecentUpdateMillis,
      LockInfo lockInfo, T task, Status status) {
    this.taskId = checkNotNull(taskId, "Null taskId");
    this.jobId = checkNotNull(jobId, "Null jobId");
    this.mostRecentUpdateMillis = mostRecentUpdateMillis;
    this.lockInfo = lockInfo;
    this.task = task;
    this.status = status;
  }

  IncrementalTaskState<T> setMostRecentUpdateMillis(long mostRecentUpdateMillis) {
    this.mostRecentUpdateMillis = mostRecentUpdateMillis;
    return this;
  }

  IncrementalTaskState<T> setSequenceNumber(int nextSequenceNumber) {
    this.sequenceNumber = nextSequenceNumber;
    return this;
  }


  int incrementAndGetRetryCount() {
    return ++retryCount;
  }

  void clearRetryCount() {
    retryCount = 0;
  }

  IncrementalTaskState<T> setTask(T task) {
    this.task = task;
    return this;
  }

  IncrementalTaskState<T> setStatus(Status status) {
    this.status = status;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + taskId + ", "
        + jobId + ", "
        + mostRecentUpdateMillis + ", "
        + sequenceNumber + ", "
        + retryCount + ", "
        + task + ", "
        + status + ", "
        + ")";
  }

  /**
   * Utility class to serialize/deserialize IncrementalTaskState.
   */
  public static class Serializer {
    static final String ENTITY_KIND = "MR-IncrementalTask";
    static final String SHARD_INFO_ENTITY_KIND = ENTITY_KIND + "-ShardInfo";

    private static final String JOB_ID_PROPERTY = "jobId";
    private static final String MOST_RECENT_UPDATE_MILLIS_PROPERTY = "mostRecentUpdateMillis";
    private static final String SEQUENCE_NUMBER_PROPERTY = "sequenceNumber";
    private static final String RETRY_COUNT_PROPERTY = "retryCount";
    private static final String SLICE_START_TIME = "sliceStartTime";
    private static final String SLICE_REQUEST_ID = "sliceRequestId";
    private static final String NEXT_TASK_PROPERTY = "nextTask";
    private static final String STATUS_PROPERTY = "status";

    public static Key makeKey(Datastore datastore, String taskId) {
      return datastore.newKeyFactory().setKind(ENTITY_KIND).newKey(taskId);
    }

    public static Entity toEntity(Transaction tx, IncrementalTaskState<?> in) {
      Key key = makeKey(tx.getDatastore(), in.getTaskId());
      Entity.Builder taskState = Entity.newBuilder(key);
      taskState.set(JOB_ID_PROPERTY, in.getJobId());
      taskState.set(MOST_RECENT_UPDATE_MILLIS_PROPERTY,
        LongValue.newBuilder(in.getMostRecentUpdateMillis()).setExcludeFromIndexes(true).build());

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

    static <T extends IncrementalTask> IncrementalTaskState<T> fromEntity(Transaction tx, Entity in) {
      return fromEntity(tx, in, false);
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

      IncrementalTaskState<T> state = new IncrementalTaskState<>(in.getKey().getName(),
          in.getString(JOB_ID_PROPERTY),
          in.getLong(MOST_RECENT_UPDATE_MILLIS_PROPERTY),
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
