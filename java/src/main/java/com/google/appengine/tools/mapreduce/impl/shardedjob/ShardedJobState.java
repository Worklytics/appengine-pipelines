// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;


import java.time.Instant;

/**
 * Information about execution and progress of a sharded job.
 *
 * Undefined behavior results if any of the values (such as the return value of
 * getSettings()) are mutated.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public interface ShardedJobState {

  /**
   * Returns the ID of this job.
   */
  ShardedJobRunId getShardedJobId();

  /**
   * Returns the execution settings of this job.
   */
  ShardedJobSettings getSettings();

  /**
   * Returns the total number of tasks (not including follow-up tasks) that this
   * job consists of.
   */
  int getTotalTaskCount();

  /**
   * Returns the number of tasks or follow-up tasks that are currently active.
   */
  int getActiveTaskCount();

  /**
   * Returns the time this job was started.
   */
  Instant getStartTime();

  /**
   * Returns the time this job's state was last updated.
   */
  Instant getMostRecentUpdateTime();

  /**
   * Returns whether this job is running, finished, etc.
   */
  Status getStatus();
}
