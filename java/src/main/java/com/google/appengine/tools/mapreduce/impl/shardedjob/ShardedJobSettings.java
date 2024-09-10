// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.modules.ModulesServiceFactory;
import lombok.Getter;
import lombok.ToString;

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



}
