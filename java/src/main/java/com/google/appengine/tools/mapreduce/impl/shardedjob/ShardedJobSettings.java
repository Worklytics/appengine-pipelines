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
import lombok.Getter;
import lombok.ToString;
import lombok.extern.java.Log;

import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapSettings.WORKER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SHARD_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_RETRIES;

import java.io.IOException;
import java.io.Serializable;

/**
 * Execution settings for a sharded job.
 *
 * @author ohler@google.com (Christian Ohler)
 */
@Log
@Getter
@ToString
public final class ShardedJobSettings implements Serializable {

  private static final long serialVersionUID = 286995366653078363L;

  public static final int DEFAULT_SLICE_TIMEOUT_MILLIS = 11 * 60000;

  //q: does this need to get bucketName / credentials?

  /*Nullable*/ private final String module;
  /*Nullable*/ private final String version;
  // TODO(ohler): Integrate with pipeline and put this under /_ah/pipeline.
  /*Nullable*/ private final String pipelineStatusUrl;
  /*Nullable*/ private final String mrStatusUrl;
  private final String controllerPath;
  private final String workerPath;
  private final String target;
  private final String queueName;
  private final int maxShardRetries;
  private final int maxSliceRetries;
  private final int sliceTimeoutMillis;

  /**
   * ShardedJobSettings builder.
   */
  public static class Builder {

    private String module;
    private String version;
    private String pipelineStatusUrl;
    private String mrStatusUrl;
    private String controllerPath = DEFAULT_BASE_URL + CONTROLLER_PATH;
    private String workerPath = DEFAULT_BASE_URL + WORKER_PATH;
    private String queueName = "default";
    private int maxShardRetries = DEFAULT_SHARD_RETRIES;
    private int maxSliceRetries = DEFAULT_SLICE_RETRIES;
    private int sliceTimeoutMillis = DEFAULT_SLICE_TIMEOUT_MILLIS;

    public Builder setPipelineStatusUrl(String pipelineStatusUrl) {
      this.pipelineStatusUrl = pipelineStatusUrl;
      return this;
    }

    public Builder setModule(String module) {
      this.module = module;
      return this;
    }

    public Builder setVersion(String version) {
      this.version = version;
      return this;
    }

    public Builder setControllerPath(String controllerPath) {
      this.controllerPath = checkNotNull(controllerPath, "Null controllerPath");
      return this;
    }

    public Builder setWorkerPath(String workerPath) {
      this.workerPath = checkNotNull(workerPath, "Null workerPath");
      return this;
    }

    public Builder setQueueName(String queueName) {
      this.queueName = checkNotNull(queueName, "Null queueName");
      return this;
    }

    public Builder setMaxShardRetries(int maxShardRetries) {
      this.maxShardRetries = maxShardRetries;
      return this;
    }

    public Builder setMaxSliceRetries(int maxSliceRetries) {
      this.maxSliceRetries = maxSliceRetries;
      return this;
    }

    public Builder setSliceTimeoutMillis(int sliceTimeoutMillis) {
      this.sliceTimeoutMillis = sliceTimeoutMillis;
      return this;
    }

    public Builder setMapReduceStatusUrl(String mrStatusUrl) {
      this.mrStatusUrl = mrStatusUrl;
      return this;
    }

    public ShardedJobSettings build() {
      return new ShardedJobSettings(controllerPath, workerPath, mrStatusUrl, pipelineStatusUrl,
          module, version, queueName, maxShardRetries, maxSliceRetries,
          sliceTimeoutMillis);
    }
  }

  private ShardedJobSettings(String controllerPath, String workerPath, String mrStatusUrl,
      String pipelineStatusUrl, String module, String version, String queueName,
      int maxShardRetries, int maxSliceRetries, int sliceTimeoutMillis) {
    this.controllerPath = controllerPath;
    this.workerPath = workerPath;
    this.mrStatusUrl = mrStatusUrl;
    this.pipelineStatusUrl = pipelineStatusUrl;
    this.module = module;
    this.version = version;
    this.queueName = queueName;
    this.maxShardRetries = maxShardRetries;
    this.maxSliceRetries = maxSliceRetries;
    this.sliceTimeoutMillis = sliceTimeoutMillis;
    target = resolveTaskQueueTarget();
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

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

    final ShardedJobSettings.Builder builder = new ShardedJobSettings.Builder()
      .setControllerPath(abstractSettings.getBaseUrl() + CONTROLLER_PATH + "/" + shardedJobRunId.asEncodedString())
      .setWorkerPath(abstractSettings.getBaseUrl() + WORKER_PATH + "/" + shardedJobRunId.asEncodedString())
      .setMapReduceStatusUrl(abstractSettings.getBaseUrl() + "detail?mapreduce_id=" + shardedJobRunId.asEncodedString())
      .setPipelineStatusUrl(PipelineServlet.makeViewerUrl(pipelineRunId, shardedJobRunId))
      .setModule(module)
      .setVersion(version)
      .setQueueName(abstractSettings.getWorkerQueueName())
      .setMaxShardRetries(abstractSettings.getMaxShardRetries())
      .setMaxSliceRetries(abstractSettings.getMaxSliceRetries())
      .setSliceTimeoutMillis(
        Math.max(DEFAULT_SLICE_TIMEOUT_MILLIS, (int) (abstractSettings.getMillisPerSlice() * abstractSettings.getSliceTimeoutRatio())));
    return RetryExecutor.call(getModulesRetryerBuilder(), () -> builder.build());
  }


}
