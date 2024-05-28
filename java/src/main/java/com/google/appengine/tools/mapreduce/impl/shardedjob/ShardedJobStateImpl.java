// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.datastore.*;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

import java.util.BitSet;

/**
 * Implements {@link ShardedJobState}, with additional package-private features.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of the IncrementalTask
 */
@Getter
@EqualsAndHashCode
class ShardedJobStateImpl<T extends IncrementalTask> implements ShardedJobState {

  private final String jobId;
  private final ShardedJobController<T> controller;
  private final ShardedJobSettings settings;
  private final int totalTaskCount;
  private final long startTimeMillis;
  private long mostRecentUpdateTimeMillis;
  private BitSet shardsCompleted;
  private Status status;


  public static <T extends IncrementalTask> ShardedJobStateImpl<T> create(String jobId,
      ShardedJobController<T> controller, ShardedJobSettings settings, int totalTaskCount,
      long startTimeMillis) {
    return new ShardedJobStateImpl<>(checkNotNull(jobId, "Null jobId"),
        checkNotNull(controller, "Null controller"), checkNotNull(settings, "Null settings"),
        totalTaskCount, startTimeMillis, new Status(StatusCode.RUNNING));
  }

  private ShardedJobStateImpl(String jobId, ShardedJobController<T> controller,
      ShardedJobSettings settings, int totalTaskCount, long startTimeMillis, Status status) {
    this.jobId = jobId;
    this.controller = controller;
    this.settings = settings;
    this.totalTaskCount = totalTaskCount;
    this.shardsCompleted = new BitSet(totalTaskCount);
    this.startTimeMillis = startTimeMillis;
    this.mostRecentUpdateTimeMillis = startTimeMillis;
    this.status = status;

  }

  @Override public int getActiveTaskCount() {
    return totalTaskCount - shardsCompleted.cardinality();
  }

  public void markShardCompleted(int shard) {
    shardsCompleted.set(shard);
  }

  private ShardedJobStateImpl<T> setShardsCompleted(BitSet shardsCompleted) {
    this.shardsCompleted = shardsCompleted;
    return this;
  }

  ShardedJobStateImpl<T> setMostRecentUpdateTimeMillis(long mostRecentUpdateTimeMillis) {
    this.mostRecentUpdateTimeMillis = mostRecentUpdateTimeMillis;
    return this;
  }

  ShardedJobStateImpl<T> setStatus(Status status) {
    this.status = checkNotNull(status, "Null status");
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + controller + ", "
        + status + ", "
        + shardsCompleted.cardinality() + "/" + totalTaskCount + ", "
        + mostRecentUpdateTimeMillis
        + ")";
  }

  static class ShardedJobSerializer {
    static final String ENTITY_KIND = "MR-ShardedJob";

    private static final String CONTROLLER_PROPERTY = "controller";
    private static final String START_TIME_PROPERTY = "startTimeMillis";
    private static final String SETTINGS_PROPERTY = "settings";
    private static final String TOTAL_TASK_COUNT_PROPERTY = "taskCount";
    private static final String MOST_RECENT_UPDATE_TIME_PROPERTY = "mostRecentUpdateTimeMillis";
    private static final String SHARDS_COMPLETED_PROPERTY = "activeShards";
    private static final String STATUS_PROPERTY = "status";

    static Key makeKey(Datastore datastore, String jobId) {
      return datastore.newKeyFactory().setKind(ENTITY_KIND).newKey(jobId);
    }

    static Entity toEntity(@NonNull Transaction tx, ShardedJobStateImpl<?> in) {
      Key key = makeKey(tx.getDatastore(), in.getJobId());
      Entity.Builder jobState = Entity.newBuilder(key);
      serializeToDatastoreProperty(tx, jobState, CONTROLLER_PROPERTY, in.getController());
      serializeToDatastoreProperty(tx, jobState, SETTINGS_PROPERTY, in.getSettings());
      serializeToDatastoreProperty(tx, jobState, SHARDS_COMPLETED_PROPERTY, in.shardsCompleted);
      serializeToDatastoreProperty(tx, jobState, STATUS_PROPERTY, in.getStatus());
      jobState.set(TOTAL_TASK_COUNT_PROPERTY, LongValue.newBuilder(in.getTotalTaskCount()).setExcludeFromIndexes(true).build());
      jobState.set(START_TIME_PROPERTY, LongValue.newBuilder(in.getStartTimeMillis()).setExcludeFromIndexes(true).build());
      jobState.set(MOST_RECENT_UPDATE_TIME_PROPERTY,
        LongValue.newBuilder(in.getMostRecentUpdateTimeMillis()).setExcludeFromIndexes(true).build());
      return jobState.build();
    }

    static <T extends IncrementalTask> ShardedJobStateImpl<T> fromEntity(@NonNull Transaction tx, Entity in) {
      return fromEntity(tx, in, false);
    }

    static <T extends IncrementalTask> ShardedJobStateImpl<T> fromEntity(
      @NonNull Transaction tx, Entity in, boolean lenient) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKey().getKind()), "Unexpected kind: %s", in);
      return new ShardedJobStateImpl<>(in.getKey().getName(),
          SerializationUtil.<ShardedJobController<T>>deserializeFromDatastoreProperty(tx, in, CONTROLLER_PROPERTY, lenient),
          SerializationUtil.<ShardedJobSettings>deserializeFromDatastoreProperty(tx, in, SETTINGS_PROPERTY),
          (int) in.getLong(TOTAL_TASK_COUNT_PROPERTY),
          in.getLong(START_TIME_PROPERTY),
          SerializationUtil.deserializeFromDatastoreProperty(tx, in, STATUS_PROPERTY))
            .setMostRecentUpdateTimeMillis(in.getLong(MOST_RECENT_UPDATE_TIME_PROPERTY))
            .setShardsCompleted((BitSet) SerializationUtil.deserializeFromDatastoreProperty(tx, in, SHARDS_COMPLETED_PROPERTY));
    }
  }
}
