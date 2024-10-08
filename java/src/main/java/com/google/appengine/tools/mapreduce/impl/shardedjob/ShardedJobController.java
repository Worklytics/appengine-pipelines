// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.tools.pipeline.PipelineService;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Aggregates results from {@link IncrementalTask}s and receives notification
 * when the job completes.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> the type of the incremental task
 */
@EqualsAndHashCode
public abstract class ShardedJobController<T extends IncrementalTask> implements Serializable {

  private static final long serialVersionUID = 6209078163062384156L;

  /**
   * Called when the sharded job has completed successfully.
   */
  public abstract void completed(Iterator<T> completedTasks);

  /**
   * Called when the sharded job has failed to complete successfully.
   * @param status
   */
  public abstract void failed(Status status);

  public abstract PipelineService getPipelineService();

  public abstract void setPipelineService(PipelineService pipelineService);

}
