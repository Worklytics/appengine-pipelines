// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.appengine.tools.pipeline.util.MemUsage;
import com.google.common.base.Preconditions;
import lombok.*;
import lombok.extern.java.Log;

import java.io.Serial;
import java.io.Serializable;


/**
 * Settings that affect how a Map job is executed.  May affect performance and
 * resource usage, but should not affect the result (unless the result is
 * dependent on the performance or resource usage of the computation, or if
 * different backends, modules or different base urls have different versions of the code).
 */
@Getter
@ToString
@RequiredArgsConstructor
@Log
@With
@lombok.Builder(toBuilder = true)
public class MapSettings implements ShardedJobAbstractSettings, Serializable {

  @Serial
  private static final long serialVersionUID = 51425056338041064L;

  private static RetryerBuilder getQueueRetryerBuilder() {
    return RetryerBuilder.newBuilder()
      .withWaitStrategy(RetryUtils.defaultWaitStrategy())
      .withStopStrategy(StopStrategies.stopAfterAttempt(8))
      .withRetryListener(RetryUtils.logRetry(log, MapSettings.class.getName()));
  }

  public static final String DEFAULT_BASE_URL = "/mapreduce/";
  public static final String CONTROLLER_PATH = "controllerCallback";
  public static final String WORKER_PATH = "workerCallback";
  public static final int DEFAULT_MILLIS_PER_SLICE = 180_000;
  public static final double DEFAULT_SLICE_TIMEOUT_RATIO = 1.1;
  public static final int DEFAULT_SHARD_RETRIES = 4;
  public static final int DEFAULT_SLICE_RETRIES = 20;



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
   * Sets the base URL that will be used for all requests related to this job.
   * Defaults to {@value #DEFAULT_BASE_URL}
   */
  @NonNull
  private final String baseUrl = DEFAULT_BASE_URL;

  /**
   * Specifies the Module (Service) that the job will run on.
   * If this is not set or {@code null}, it will run on the current module (service).
   *
   * in appengine gen2, these are called services
   */
  private final String module;

  /**
   * the TaskQueue that will be used to queue the job's tasks.
   */
  private final String workerQueueName;

  /**
   * How long a worker will process items before endSlice is called and progress is check-pointed to datastore.
   *
   * NOTE: if 0/negative, then will be check-pointed after each item.
   */
  @lombok.Builder.Default
  private final int millisPerSlice = DEFAULT_MILLIS_PER_SLICE;

  /**
   * A ratio for how much time beyond millisPerSlice must elapse before slice will be
   * considered to have failed due to a timeout.
   */
  @lombok.Builder.Default
  private final double sliceTimeoutRatio = DEFAULT_SLICE_TIMEOUT_RATIO;


  /**
   * Threshold for high memory usage. If the memory usage is above this threshold, the worker will proactively
   * checkpoint.
   *
   */
  @lombok.Builder.Default
  private final Double workerHighMemUsagePercent = MemUsage.DEFAULT_THRESHOLD;

  /**
   * The number of times a Shard can fail before it gives up and fails the whole job.
   */
  @lombok.Builder.Default
  private final int maxShardRetries = DEFAULT_SHARD_RETRIES;

  /**
   * The number of times a Slice can fail before triggering a shard retry.
   */
  @lombok.Builder.Default
  private final int maxSliceRetries = DEFAULT_SLICE_RETRIES;

  /**
   * a static extended Builder class, which gives us two things:
   *   1) replicates legacy validation logic per-parameter, as each setter is called on the builder
   *   2) name then matches how it was named in the legacy class (MapSettings.Builder, rather than MapSettings.MapSettingsBuilder)
   *
   *  however, it will NOT do it for the toBuilder() case. (q: how to solve this??)
   *
   */
  public static Builder builder() {
    return new Builder();
  }


  public static class Builder extends MapSettings.MapSettingsBuilder {

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
  }
}
