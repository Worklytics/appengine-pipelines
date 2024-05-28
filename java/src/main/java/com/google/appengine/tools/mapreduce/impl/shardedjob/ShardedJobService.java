// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Transaction;

import java.util.Iterator;
import java.util.List;

/**
 * Allows interaction with sharded jobs.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public interface ShardedJobService {

  /**
   * Starts a new sharded job with the given ID and parameters.  The ID must
   * be unique.
   * <p>
   * This method is idempotent -- if another invocation of this method aborted
   * (or is in an unknown state, possibly still running or completed), starting
   * the job can be retried by calling the method again with the same arguments.
   * The job won't start twice unless {@link #cleanupJob} is called in between.
   *
   * @param <T>       type of tasks that the job consists of
   * @param datastore
   */
  <T extends IncrementalTask> void startJob(
    Datastore datastore, String jobId,
    List<? extends T> initialTasks,
    ShardedJobController<T> controller,
    ShardedJobSettings settings);

  /**
   * Returns the state of the job with the given ID.  Returns null if no such
   * job exists.
   */
  ShardedJobState getJobState(Datastore datastore, String jobId);

  /**
   * Returns the tasks associated with this ShardedJob.
   */
  Iterator<IncrementalTaskState<IncrementalTask>> lookupTasks(Transaction tx, ShardedJobState state);

  /**
   * Aborts execution of the job with the given ID.  If the job has already
   * finished or does not exist, this is a no-op.
   */
  void abortJob(Datastore datastore, String jobId);

  /**
   * Deletes all data of a completed job with the given ID.
   * Data is being deleted asynchronously.
   * Returns true if job was already deleted or asynchronous task was submitted successfully.
   */
  boolean cleanupJob(Datastore datastore, String jobId);
}
