// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.With;
import lombok.extern.java.Log;

import java.io.Serial;
import java.io.Serializable;
import java.util.Optional;

/**
 * Settings that affect how a MapReduce is executed. May affect performance and resource usage, but
 * should not affect the result (unless the result is dependent on the performance or resource usage
 * of the computation, or if different backends, modules or different base urls have different
 * versions of the code).
 *
 * @author ohler@google.com (Christian Ohler)
 */
@Log
@Getter
@ToString
@With
@lombok.Builder(toBuilder = true)
public class MapReduceSettings implements GcpCredentialOptions, ShardedJobAbstractSettings, Serializable {

  @Serial
  private static final long serialVersionUID = 610088354289299175L;

  public static final int DEFAULT_MAP_FANOUT = 32;
  public static final int DEFAULT_SORT_BATCH_PER_EMIT_BYTES = 32 * 1024;
  public static final int DEFAULT_SORT_READ_TIME_MILLIS = 180000;
  public static final int DEFAULT_MERGE_FANIN = 32;

  /**
   * The host name of the datastore to use for all requests related to this job.
   *  (use case: local emulation)
   */
  private final String datastoreHost;

  /**
   * The project that the job will run in.
   */
  private final String projectId;

  /**
   * The database within the project to which the job will persist its state data.
   */
  private final String databaseId;

  /**
   * The namespace within the database to which the job will persist its state data.
   */
  private final String namespace;

  @lombok.Builder.Default
  /**
   * The base URL that will be used for all requests related to this job.
   * Defaults to {@value #DEFAULT_BASE_URL}
   */
  @NonNull
  private final String baseUrl = MapSettings.DEFAULT_BASE_URL;

  /**
   * The Module (Service) that the job will run on.
   *
   * If this is not set or {@code null}, it will run on the current module (service).
   *
   * in appengine gen2, these are called services
   */
  private final String module;

  /**
   * The TaskQueue that will be used to queue the job's tasks.
   */
  private final String workerQueueName;

  /**
   * How long a worker will process items before endSlice is called and progress is check-pointed to datastore.
   *
   * NOTE: if 0/negative, then will be check-pointed after each item.
   */
  @lombok.Builder.Default
  private final int millisPerSlice = MapSettings.DEFAULT_MILLIS_PER_SLICE;

  /**
   * The ratio for how much time beyond millisPerSlice must elapse before slice will be
   * considered to have failed due to a timeout.
   */
  @lombok.Builder.Default
  private final double sliceTimeoutRatio= MapSettings.DEFAULT_SLICE_TIMEOUT_RATIO;

  /**
   * The number of times a Shard can fail before it gives up and fails the whole job.
   */
  @lombok.Builder.Default
  private final int maxShardRetries = MapSettings.DEFAULT_SHARD_RETRIES;

  /**
   * The number of times a Slice can fail before triggering a shard retry.
   */
  @lombok.Builder.Default
  private final int maxSliceRetries = MapSettings.DEFAULT_SLICE_RETRIES;

  /**
   * The GCS bucket that will be used for temporary files.
   *
   * now REQUIRED to be filled by settings; will not use a default
   */
  @NonNull
  private final String bucketName;

  /**
   * The maximum number of files the map stage will write to at the same time. A higher number may
   * increase the speed of the job at the expense of more memory used during the map and sort
   * phases and more intermediate files created.
   *
   * Using the default is recommended.
   */
  @lombok.Builder.Default
  private int mapFanout = DEFAULT_MAP_FANOUT;

  /**
   * The maximum memory the sort stage should allocate (in bytes). This is used to lower the
   * amount of memory it will use. Regardless of this setting it will not exhaust available
   * memory. Null or unset will use the default (no maximum)
   *
   * Using the default is recommended.
   */
  private final Long maxSortMemory;


  /**
   * The maximum length of time sort should spend reading input before it starts sorting it and
   * writing it out.
   *
   * Using the default is recommended.
   */
  @lombok.Builder.Default
  private int sortReadTimeMillis = DEFAULT_SORT_READ_TIME_MILLIS;

  /**
   * Size (in bytes) of items to batch together in the output of the sort. (A higher value saves
   * storage cost, but needs to be small enough to not impact memory use.)
   *
   * Using the default is recommended.
   */
  @lombok.Builder.Default
  private int sortBatchPerEmitBytes = DEFAULT_SORT_BATCH_PER_EMIT_BYTES;

  /**
   * Number of files the merge stage will read at the same time. A higher number can increase the
   * speed of the job at the expense of requiring more memory in the merge stage.
   *
   * Using the default is recommended.
   */
  @lombok.Builder.Default
  private int mergeFanin = DEFAULT_MERGE_FANIN;

  /**
   * credentials to use when accessing storage for sort/shuffle phases of this MR j
   *
   * NOTE: as these will be serialized / copied to datastore/etc during pipeline execution, this exists mainly for dev
   * purposes where you're running outside a GCP environment where the default implicit credentials will work. For
   * production use, relying on implicit credentials and granting the service account under which your code is executing
   * in GAE/GCE/etc is the most secure approach, as no keys need to be generated or passed around, which always entails
   * some risk of exposure.
   */
  private final String serviceAccountKey;

  /**
   * a static extended Builder class, which gives us two things:
   *   1) replicates legacy validation logic per-parameter, as each setter is called on the builder
   *   2) name then matches how it was named in the legacy class (MapSettings.Builder, rather than MapSettings.MapSettingsBuilder)
   *
   */

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MapReduceSettings.MapReduceSettingsBuilder {

    @Override
    public Builder sliceTimeoutRatio(double sliceTimeoutRatio) {
      Preconditions.checkArgument(sliceTimeoutRatio > 1.0, "sliceTimeoutRatio must be greater than 1.0");
      super.sliceTimeoutRatio(sliceTimeoutRatio);
      return this;
    }

    @Override
    public Builder maxShardRetries(int maxShardRetries) {
      Preconditions.checkArgument(maxShardRetries > -1, "maxShardRetries cannot be negative");
      super.maxShardRetries(maxShardRetries);
      return this;
    }

    @Override
    public Builder maxSliceRetries(int maxSliceRetries) {
      Preconditions.checkArgument(maxSliceRetries > -1, "maxSliceRetries cannot be negative");
      super.maxSliceRetries(maxSliceRetries);
      return this;
    }

    @Override
    public Builder workerQueueName(String workerQueueName) {
      super.workerQueueName(workerQueueName);
      return this;
    }

    @Override
    public Builder maxSortMemory(Long maxSortMemory) {
      Preconditions.checkArgument(maxSortMemory > -1L, "maxSortMemory cannot be negative");
      super.maxSortMemory(maxSortMemory);
      return this;
    }

    @Override
    public Builder mergeFanin(int mergeFanin) {
      Preconditions.checkArgument(mergeFanin > -1, "mergeFanin cannot be negative");
      super.mergeFanin(mergeFanin);
      return this;
    }

    @Override
    public Builder sortBatchPerEmitBytes(int sortBatchPerEmitBytes) {
      Preconditions.checkArgument(sortBatchPerEmitBytes > -1, "sortBatchPerEmitBytes cannot be negative");
      super.sortBatchPerEmitBytes(sortBatchPerEmitBytes);
      return this;
    }

    @Override
    public Builder sortReadTimeMillis(int sortReadTimeMillis) {
      Preconditions.checkArgument(sortReadTimeMillis > -1, "sortReadTimeMillis cannot be negative");
      super.sortReadTimeMillis(sortReadTimeMillis);
      return this;
    }
  }
}

