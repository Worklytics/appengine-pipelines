// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.java.Log;

import java.time.Instant;
import java.util.BitSet;
import java.util.Date;

/**
 * Implements {@link ShardedJobState}, with additional package-private features.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of the IncrementalTask
 */
@Log
@Getter
@EqualsAndHashCode
class ShardedJobStateImpl<T extends IncrementalTask> implements ShardedJobState {

  private final ShardedJobId shardedJobId;
  private final ShardedJobController<T> controller;
  private final ShardedJobSettings settings;
  private final int totalTaskCount;
  private final Instant startTime;
  private Instant mostRecentUpdateTime;
  private BitSet shardsCompleted;
  private Status status;


  public static <T extends IncrementalTask> ShardedJobStateImpl<T> create(
                                                                          @NonNull String project,
                                                                          @NonNull String namespace,
                                                                          @NonNull String generatedJobId,
                                                                          @NonNull ShardedJobController<T> controller,
                                                                          @NonNull ShardedJobSettings settings,
                                                                          int totalTaskCount,
                                                                          Instant startTime) {

    ShardedJobId jobId = ShardedJobId.of(project, namespace, generatedJobId);
    return new ShardedJobStateImpl<>(jobId, controller, settings, totalTaskCount, startTime, new Status(StatusCode.RUNNING));
  }

  public static <T extends IncrementalTask> ShardedJobStateImpl<T> create(
            @NonNull ShardedJobId shardedJobId,
            @NonNull ShardedJobController<T> controller,
            @NonNull ShardedJobSettings settings,
            int totalTaskCount,
            @NonNull Instant startTime

      ) {
    return new ShardedJobStateImpl<>(shardedJobId, controller, settings, totalTaskCount, startTime, new Status(StatusCode.RUNNING));
  }


  private ShardedJobStateImpl(ShardedJobId shardedJobId,
                              ShardedJobController<T> controller,
                              ShardedJobSettings settings,
                              int totalTaskCount,
                              Instant startTime,
                              Status status) {
    this.shardedJobId = shardedJobId;
    this.controller = controller;
    this.settings = settings;
    this.totalTaskCount = totalTaskCount;
    this.shardsCompleted = new BitSet(totalTaskCount);
    this.startTime = startTime;
    this.mostRecentUpdateTime = startTime;
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

  ShardedJobStateImpl<T> setMostRecentUpdateTime(Instant mostRecentUpdateTime) {
    this.mostRecentUpdateTime = mostRecentUpdateTime;
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
        + mostRecentUpdateTime
        + ")";
  }

  static class ShardedJobSerializer {
    static final String ENTITY_KIND = "MR-ShardedJob";

    private static final String CONTROLLER_PROPERTY = "controller";
    private static final String START_TIME_PROPERTY = "startTime";
    private static final String SETTINGS_PROPERTY = "settings";
    private static final String TOTAL_TASK_COUNT_PROPERTY = "taskCount";
    private static final String MOST_RECENT_UPDATE_TIME_PROPERTY = "mostRecentUpdateTime";
    private static final String SHARDS_COMPLETED_PROPERTY = "activeShards";
    private static final String STATUS_PROPERTY = "status";

    static Key makeKey(Datastore datastore, ShardedJobId jobId) {
      KeyFactory builder = datastore.newKeyFactory().setKind(ENTITY_KIND)
        //.setDatabaseId(pipelineKey.getDatabaseId()) //TODO: uncomment this if going to support databaseId
        .setProjectId(jobId.getProject())
        .setNamespace(jobId.getNamespace());
      return builder.newKey(jobId.getJobId());
    }

    static Entity toEntity(@NonNull Transaction tx, ShardedJobStateImpl<?> in) {
      Key key = makeKey(tx.getDatastore(), in.getShardedJobId());
      Entity.Builder jobState = Entity.newBuilder(key);

      //avoid serialization issue; will fill on deserialization
      in.getController().setPipelineService(null);
      serializeToDatastoreProperty(tx, jobState, CONTROLLER_PROPERTY, in.getController());
      serializeToDatastoreProperty(tx, jobState, SETTINGS_PROPERTY, in.getSettings());
      serializeToDatastoreProperty(tx, jobState, SHARDS_COMPLETED_PROPERTY, in.shardsCompleted);
      serializeToDatastoreProperty(tx, jobState, STATUS_PROPERTY, in.getStatus());
      jobState.set(TOTAL_TASK_COUNT_PROPERTY, LongValue.newBuilder(in.getTotalTaskCount()).setExcludeFromIndexes(true).build());
      jobState.set(START_TIME_PROPERTY, timestampBuilder(in.getStartTime()).setExcludeFromIndexes(true).build());
      jobState.set(MOST_RECENT_UPDATE_TIME_PROPERTY,
        timestampBuilder(in.getMostRecentUpdateTime()).setExcludeFromIndexes(true).build());
      return jobState.build();
    }

    static TimestampValue.Builder timestampBuilder(Instant instant) {
      return TimestampValue.newBuilder(Timestamp.of(Date.from(instant)));
    }

    static Instant from(Timestamp timestamp) {
      return timestamp.toDate().toInstant();
    }

    static <T extends IncrementalTask> ShardedJobStateImpl<T> fromEntity(@NonNull Transaction tx, Entity in) {
      return fromEntity(tx, in, false);
    }

    static <T extends IncrementalTask> ShardedJobStateImpl<T> fromEntity(
      @NonNull Transaction tx, Entity in, boolean lenient) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKey().getKind()), "Unexpected kind: %s", in);

      Key sharedJobStateKey = in.getKey();

      ShardedJobId jobId = ShardedJobId.of(sharedJobStateKey.getProjectId(), sharedJobStateKey.getNamespace(), sharedJobStateKey.getName());

      return new ShardedJobStateImpl<>(
          jobId,
          SerializationUtil.<ShardedJobController<T>>deserializeFromDatastoreProperty(tx, in, CONTROLLER_PROPERTY, lenient),
          SerializationUtil.<ShardedJobSettings>deserializeFromDatastoreProperty(tx, in, SETTINGS_PROPERTY),
          (int) in.getLong(TOTAL_TASK_COUNT_PROPERTY),
          from(in.getTimestamp(START_TIME_PROPERTY)),
          SerializationUtil.deserializeFromDatastoreProperty(tx, in, STATUS_PROPERTY))
            .setMostRecentUpdateTime(from(in.getTimestamp(MOST_RECENT_UPDATE_TIME_PROPERTY)))
            .setShardsCompleted(SerializationUtil.deserializeFromDatastoreProperty(tx, in, SHARDS_COMPLETED_PROPERTY));
    }
  }
}
