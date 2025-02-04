// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Preconditions;
import lombok.*;
import lombok.extern.java.Log;

import java.time.Instant;
import java.util.BitSet;
import java.util.Date;
import java.util.Optional;

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
@Builder(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
class ShardedJobStateImpl<T extends IncrementalTask> implements ShardedJobState {

  private final ShardedJobRunId shardedJobId;
  private final ShardedJobController<T> controller;
  private final ShardedJobSettings settings;
  private final int totalTaskCount;
  @NonNull
  private final Instant startTime;
  private Instant mostRecentUpdateTime;
  private BitSet shardsCompleted;
  private Status status;

  // used internally to track serialization of these values in the datastore
  @Getter(AccessLevel.PRIVATE)
  @Setter(AccessLevel.PRIVATE)
  @Builder.Default
  private int statusShards = 0;
  @Getter(AccessLevel.PRIVATE)
  @Setter(AccessLevel.PRIVATE)
  @Builder.Default
  private int controllerShards = 0;
  @Getter(AccessLevel.PRIVATE)
  @Setter(AccessLevel.PRIVATE)
  @Builder.Default
  private int settingsShards = 0;


  public static <T extends IncrementalTask> ShardedJobStateImpl<T> create(
    @NonNull String project,
    @NonNull String databaseId,
    @NonNull String namespace,
    @NonNull String generatedJobId,
    @NonNull ShardedJobController<T> controller,
    @NonNull ShardedJobSettings settings,
    int totalTaskCount,
    @NonNull Instant startTime) {

    ShardedJobRunId jobId = ShardedJobRunId.of(project, databaseId, namespace, generatedJobId);

    return create(jobId, controller, settings, totalTaskCount, startTime);
  }

  public static <T extends IncrementalTask> ShardedJobStateImpl<T> create(
    @NonNull ShardedJobRunId shardedJobId,
    @NonNull ShardedJobController<T> controller,
    @NonNull ShardedJobSettings settings,
    int totalTaskCount,
    @NonNull Instant startTime
  ) {

    return ShardedJobStateImpl.<T>builder()
      .shardedJobId(shardedJobId)
      .controller(controller)
      .settings(settings)
      .shardsCompleted(new BitSet(totalTaskCount))
      .totalTaskCount(totalTaskCount)
      .startTime(startTime)
      .status(new Status(StatusCode.RUNNING))
      .build();
  }


  @Override
  public int getActiveTaskCount() {
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

    static Key makeKey(Datastore datastore, ShardedJobRunId jobId) {
      KeyFactory builder = datastore.newKeyFactory()
        .setKind(ENTITY_KIND)
        .setProjectId(jobId.getProject());

      // null implies default? unset certainly does, so we'll leave that
      if (jobId.getDatabaseId() != null) {
        builder.setDatabaseId(jobId.getDatabaseId());
      }

      // null implies default, but datastore client wants left unset in that case
      if (jobId.getNamespace() != null) {
        builder.setNamespace(jobId.getNamespace());
      }
      return builder.newKey(jobId.getJobId());
    }

    static Entity toEntity(@NonNull Transaction tx, ShardedJobStateImpl<?> in) {
      Key key = makeKey(tx.getDatastore(), in.getShardedJobId());
      Entity.Builder jobState = Entity.newBuilder(key);

      //avoid serialization issue; will fill on deserialization
      in.getController().setPipelineService(null);
      serializeToDatastoreProperty(tx, jobState, CONTROLLER_PROPERTY, in.getController(), Optional.of(in.controllerShards));
      serializeToDatastoreProperty(tx, jobState, SETTINGS_PROPERTY, in.getSettings(), Optional.of(in.settingsShards));
      serializeToDatastoreProperty(tx, jobState, SHARDS_COMPLETED_PROPERTY, in.shardsCompleted, Optional.of(0)); // this is a BitSet, so assume will NEVER exceed blob limit (as that'd be nuts)
      serializeToDatastoreProperty(tx, jobState, STATUS_PROPERTY, in.getStatus(), Optional.of(in.statusShards));
      jobState.set(TOTAL_TASK_COUNT_PROPERTY, LongValue.newBuilder(in.getTotalTaskCount()).setExcludeFromIndexes(true).build());
      jobState.set(START_TIME_PROPERTY, timestampBuilder(in.getStartTime()).setExcludeFromIndexes(true).build());
      jobState.set(MOST_RECENT_UPDATE_TIME_PROPERTY,
          timestampBuilder(Optional.ofNullable(in.getMostRecentUpdateTime()).orElse(Instant.now())).setExcludeFromIndexes(true).build());

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

      ShardedJobRunId jobId = ShardedJobRunId.of(in.getKey());

      return ShardedJobStateImpl.<T>builder()
        .shardedJobId(jobId)
        .controller(SerializationUtil.deserializeFromDatastoreProperty(tx, in, CONTROLLER_PROPERTY, lenient))
        .controllerShards(SerializationUtil.shardsUsedToStore(in, CONTROLLER_PROPERTY))
        .settings(SerializationUtil.deserializeFromDatastoreProperty(tx, in, SETTINGS_PROPERTY))
        .settingsShards(SerializationUtil.shardsUsedToStore(in, SETTINGS_PROPERTY))
        .totalTaskCount((int) in.getLong(TOTAL_TASK_COUNT_PROPERTY))
        .startTime(from(in.getTimestamp(START_TIME_PROPERTY)))
        .status(SerializationUtil.deserializeFromDatastoreProperty(tx, in, STATUS_PROPERTY))
        .statusShards(SerializationUtil.shardsUsedToStore(in, STATUS_PROPERTY))
        .mostRecentUpdateTime(in.contains(MOST_RECENT_UPDATE_TIME_PROPERTY) ? from(in.getTimestamp(MOST_RECENT_UPDATE_TIME_PROPERTY)) : null)
        .shardsCompleted(SerializationUtil.deserializeFromDatastoreProperty(tx, in, SHARDS_COMPLETED_PROPERTY))
        .build();
    }
  }
}
