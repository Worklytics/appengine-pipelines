// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.appengine.api.modules.ModulesException;
import com.google.appengine.api.modules.ModulesService;
import com.google.appengine.api.modules.ModulesServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TransientFailureException;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.cloud.datastore.DatastoreOptions;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.java.Log;

import java.io.Serial;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings.DEFAULT_SLICE_TIMEOUT_MILLIS;

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
@SuperBuilder(toBuilder = true)
public class MapSettings implements Serializable {

  @Serial
  private static final long serialVersionUID = 51425056338041064L;

  private static RetryerBuilder getQueueRetryerBuilder() {
    return RetryerBuilder.newBuilder()
      .withWaitStrategy(RetryUtils.defaultWaitStrategy())
      .withStopStrategy(StopStrategies.stopAfterAttempt(8))
      .retryIfExceptionOfType(TransientFailureException.class)
      .withRetryListener(RetryUtils.logRetry(log, MapSettings.class.getName()));
  }

  private static RetryerBuilder getModulesRetryerBuilder() {
    return RetryerBuilder.newBuilder()
      .withWaitStrategy(RetryUtils.defaultWaitStrategy())
      .withStopStrategy(StopStrategies.stopAfterAttempt(8))
      .retryIfExceptionOfType(ModulesException.class)
      .withRetryListener(RetryUtils.logRetry(log, MapSettings.class.getName()));
  }

  public static final String DEFAULT_BASE_URL = "/mapreduce/";
  public static final String CONTROLLER_PATH = "controllerCallback";
  public static final String WORKER_PATH = "workerCallback";
  public static final int DEFAULT_MILLIS_PER_SLICE = 180_000;
  public static final double DEFAULT_SLICE_TIMEOUT_RATIO = 1.1;
  public static final int DEFAULT_SHARD_RETRIES = 4;
  public static final int DEFAULT_SLICE_RETRIES = 20;

  private final String datastoreHost;
  private final String projectId;
  private final String databaseId;
  /**
   * Sets the namespace that will be used for all requests related to this job.
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
   * Sets the TaskQueue that will be used to queue the job's tasks.
   */
  private final String workerQueueName;

  /**
   * Sets how long a worker will process items before endSlice is called and progress is
   * checkpointed to datastore.
   */
  @lombok.Builder.Default
  private final int millisPerSlice = DEFAULT_MILLIS_PER_SLICE;

  /**
   * Sets a ratio for how much time beyond millisPerSlice must elapse before slice will be
   * considered to have failed due to a timeout.
   */
  @lombok.Builder.Default
  private final double sliceTimeoutRatio= DEFAULT_SLICE_TIMEOUT_RATIO;

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


  public JobSetting[] toJobSettings(JobSetting... extra) {
    JobSetting[] settings = new JobSetting[3 + extra.length];
    settings[0] = new JobSetting.OnService(module);
    settings[1] = new JobSetting.OnQueue(workerQueueName);
    settings[2] = new JobSetting.DatastoreNamespace(namespace);
    System.arraycopy(extra, 0, settings, 3, extra.length);
    return settings;
  }

  ShardedJobSettings toShardedJobSettings(ShardedJobRunId shardedJobId, JobRunId pipelineRunId) {

    String module = getModule();
    String version = null;
    if (module == null) {
      ModulesService modulesService = ModulesServiceFactory.getModulesService();
      module = modulesService.getCurrentModule();
      version = modulesService.getCurrentVersion();
    } else {
      final ModulesService modulesService = ModulesServiceFactory.getModulesService();
      if (module.equals(modulesService.getCurrentModule())) {
        version = modulesService.getCurrentVersion();
      } else {
        // TODO(user): we may want to support providing a version for a module
        final String requestedModule = module;

        version = RetryExecutor.call(getModulesRetryerBuilder(), () -> modulesService.getDefaultVersion(requestedModule));
      }
    }

    final ShardedJobSettings.Builder builder = new ShardedJobSettings.Builder()
        .setControllerPath(baseUrl + CONTROLLER_PATH + "/" + shardedJobId.asEncodedString())
        .setWorkerPath(baseUrl + WORKER_PATH + "/" + shardedJobId.asEncodedString())
        .setMapReduceStatusUrl(baseUrl + "detail?mapreduce_id=" + shardedJobId.asEncodedString())
        .setPipelineStatusUrl(PipelineServlet.makeViewerUrl(pipelineRunId, shardedJobId))
        .setModule(module)
        .setVersion(version)
        .setQueueName(workerQueueName)
        .setMaxShardRetries(maxShardRetries)
        .setMaxSliceRetries(maxSliceRetries)
        .setSliceTimeoutMillis(
            Math.max(DEFAULT_SLICE_TIMEOUT_MILLIS, (int) (millisPerSlice * sliceTimeoutRatio)));
    return RetryExecutor.call(getModulesRetryerBuilder(), () -> builder.build());
  }

  private String checkQueueSettings(String queueName) {
    if (queueName == null) {
      return null;
    }
    final Queue queue = QueueFactory.getQueue(queueName);
    try {
      // Does not work as advertise (just check that the queue name is valid).
      // See b/13910616. Probably after the bug is fixed the check would need
      // to inspect EnforceRate for not null.
      RetryExecutor.call(getQueueRetryerBuilder(), () -> {
          // Does not work as advertise (just check that the queue name is valid).
          // See b/13910616. Probably after the bug is fixed the check would need
          // to inspect EnforceRate for not null.
          queue.fetchStatistics();
          return null;
        });
    } catch (Throwable ex) {
      if (ex instanceof ExecutionException) {
        if (ex.getCause() instanceof IllegalStateException) {
          throw new RuntimeException("Queue '" + queueName + "' does not exists");
        }
        throw new RuntimeException(
          "Could not check if queue '" + queueName + "' exists", ex.getCause());
      } else {
        throw ex;
      }
    }
    return queueName;
  }

  DatastoreOptions getDatastoreOptions() {
    DatastoreOptions.Builder optionsBuilder = DatastoreOptions.getDefaultInstance().toBuilder();
    Optional.ofNullable(datastoreHost).ifPresent(optionsBuilder::setHost);
    Optional.ofNullable(projectId).ifPresent(optionsBuilder::setProjectId);
    Optional.ofNullable(databaseId).ifPresent(optionsBuilder::setDatabaseId);
    Optional.ofNullable(namespace).ifPresent(optionsBuilder::setNamespace);
    return optionsBuilder.build();
  }
}
