// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;


import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;

/**
 * MapReduce context.
 */
public interface Context {

  /**
   * Returns the Id for the job.
   * @return the id for the job
   */
  ShardedJobRunId getJobId();
}
