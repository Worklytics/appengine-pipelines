// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.tools.mapreduce.ShardedJobAbstractSettings;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import lombok.*;
import lombok.extern.java.Log;

import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapSettings.WORKER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SHARD_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_RETRIES;

import java.io.Serial;
import java.io.Serializable;

/**
 * Execution settings for a sharded job.
 *
 * @author ohler@google.com (Christian Ohler)
 */
@AllArgsConstructor
@Builder(toBuilder = true)
@Log
@Getter
@ToString
public final class ShardedJobSettings implements Serializable {

  @Serial
  private static final long serialVersionUID = 286995366653078363L;

  public static final int DEFAULT_SLICE_TIMEOUT_MILLIS = 11 * 60000;

  //q: does this need to get bucketName / credentials?

  /** Nullable **/
  private final String module;
  /*Nullable*/ private final String version;
  // TODO(ohler): Integrate with pipeline and put this under /_ah/pipeline.
  /*Nullable*/ private final String pipelineStatusUrl;
  /*Nullable*/ private final String mrStatusUrl;
  @lombok.Builder.Default
  private String controllerPath = DEFAULT_BASE_URL + CONTROLLER_PATH;
  
  @lombok.Builder.Default
  private String workerPath = DEFAULT_BASE_URL + WORKER_PATH;
  
  @lombok.Builder.Default
  private final String queueName  = "default";

  @lombok.Builder.Default
  private final int maxShardRetries = DEFAULT_SHARD_RETRIES;
  
  @lombok.Builder.Default
  private final int maxSliceRetries = DEFAULT_SLICE_RETRIES;
  
  @lombok.Builder.Default
  private final int sliceTimeoutMillis = DEFAULT_SLICE_TIMEOUT_MILLIS;

  /*Nullable*/ public String getMapReduceStatusUrl() {
    return mrStatusUrl;
  }

  public static ShardedJobSettings from(PipelineService pipelineService,
                                        ShardedJobAbstractSettings abstractSettings, ShardedJobRunId shardedJobRunId, JobRunId pipelineRunId) {
    String module = abstractSettings.getModule();
    if (module == null) {
      module = pipelineService.getDefaultWorkerService();
    }

    String version = pipelineService.getCurrentVersion(module);

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
    return builder.build();
  }


}
