// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.tools.pipeline.PipelineService;
import lombok.AllArgsConstructor;

/**
 * Provides {@link ShardedJobService} implementations.
 *
 * @author ohler@google.com (Christian Ohler)
 */
@AllArgsConstructor
public class ShardedJobServiceFactory {

  PipelineService pipelineService;

  public ShardedJobService getShardedJobService() {
    return new ShardedJobServiceImpl(pipelineService);
  }
}
