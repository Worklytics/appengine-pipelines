// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.appengine.api.modules.ModulesException;
import com.google.appengine.api.modules.ModulesService;
import com.google.appengine.api.modules.ModulesServiceFactory;
import com.google.appengine.tools.mapreduce.MapSettings;
import com.google.appengine.tools.mapreduce.RetryExecutor;
import com.google.appengine.tools.mapreduce.RetryUtils;
import com.google.appengine.tools.mapreduce.ShardedJobAbstractSettings;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.java.Log;

import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapSettings.WORKER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SHARD_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_RETRIES;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;

/**
 * Execution settings for a sharded job.
 *
 * @author ohler@google.com (Christian Ohler)
 */
@AllArgsConstructor
@Builder
@Log
@Getter
@ToString
public final class ShardedJobSettings implements Serializable {

  @Serial
  private static final long serialVersionUID = 286995366653078363L;

  public static final int DEFAULT_SLICE_TIMEOUT_MILLIS = 11 * 60000;

  //q: does this need to get bucketName / credentials?

  /*Nullable*/ private final String module;
  /*Nullable*/ private final String version;
  // TODO(ohler): Integrate with pipeline and put this under /_ah/pipeline.
  /*Nullable*/ private final String pipelineStatusUrl;
  /*Nullable*/ private final String mrStatusUrl;
  @lombok.Builder.Default
  private String controllerPath = DEFAULT_BASE_URL + CONTROLLER_PATH;
  
  @lombok.Builder.Default
  private String workerPath = DEFAULT_BASE_URL + WORKER_PATH;
  
  @Builder.ObtainVia(method = "resolveTaskQueueTarget")
  private final String target;
  
  @lombok.Builder.Default
  private final String queueName  = "default";

  @lombok.Builder.Default
  private final int maxShardRetries = DEFAULT_SHARD_RETRIES;
  
  @lombok.Builder.Default
  private final int maxSliceRetries = DEFAULT_SLICE_RETRIES;
  
  @lombok.Builder.Default
  private final int sliceTimeoutMillis = DEFAULT_SLICE_TIMEOUT_MILLIS;

  private String resolveTaskQueueTarget() {
    return ModulesServiceFactory.getModulesService().getVersionHostname(module, version);
  }

  public String getTaskQueueTarget() {
    return target;
  }

  /*Nullable*/ public String getMapReduceStatusUrl() {
    return mrStatusUrl;
  }


  private static RetryerBuilder getModulesRetryerBuilder() {
    return RetryerBuilder.newBuilder()
      .withWaitStrategy(RetryUtils.defaultWaitStrategy())
      .withStopStrategy(StopStrategies.stopAfterAttempt(8))
      .retryIfExceptionOfType(ModulesException.class)
      .withRetryListener(RetryUtils.logRetry(log, MapSettings.class.getName()));
  }

  public static ShardedJobSettings from(ShardedJobAbstractSettings abstractSettings, ShardedJobRunId shardedJobRunId, JobRunId pipelineRunId) {
    String module = abstractSettings.getModule();
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

    ShardedJobSettings.ShardedJobSettingsBuilder builder = ShardedJobSettings.builder()
      .controllerPath(abstractSettings.getBaseUrl() + CONTROLLER_PATH + "/" + shardedJobRunId.asEncodedString())
      .workerPath(abstractSettings.getBaseUrl() + WORKER_PATH + "/" + shardedJobRunId.asEncodedString())
      .mrStatusUrl(abstractSettings.getBaseUrl() + "detail?mapreduce_id=" + shardedJobRunId.asEncodedString())
      .pipelineStatusUrl(PipelineServlet.makeViewerUrl(pipelineRunId, shardedJobRunId))
      .module(module)
      .version(version)
      .queueName(abstractSettings.getWorkerQueueName())
      .maxShardRetries(abstractSettings.getMaxShardRetries())
      .maxSliceRetries(abstractSettings.getMaxSliceRetries())
      .sliceTimeoutMillis(
        Math.max(DEFAULT_SLICE_TIMEOUT_MILLIS, (int) (abstractSettings.getMillisPerSlice() * abstractSettings.getSliceTimeoutRatio())));
    return RetryExecutor.call(getModulesRetryerBuilder(), () -> builder.build());
  }


}
