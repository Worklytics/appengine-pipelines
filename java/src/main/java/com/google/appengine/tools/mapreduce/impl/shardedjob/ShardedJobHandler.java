package com.google.appengine.tools.mapreduce.impl.shardedjob;

/**
 * As part of its operation, the {@code ShardedJobService} will enqueue task
 * queue tasks that send requests to the URLs specified in
 * {@link ShardedJobSettings}.  It is the user's responsibility to arrange
 * for these requests to be passed back into {@link #completeShard}
 * and {@link #runTask}.
 */
public interface ShardedJobHandler {

  String JOB_ID_PARAM = "job";
  String TASK_ID_PARAM = "task";
  String SEQUENCE_NUMBER_PARAM = "seq";

  /**
   * Is invoked by the servlet that handles
   * {@link ShardedJobSettings#getControllerPath} when a shard has completed.
   */
  void completeShard(final ShardedJobRunId jobId, final IncrementalTaskId taskId);

  /**
   * Is invoked by the servlet that handles the work. this actually calls run() method of the incremental task, doing something
   * that is potentially long-running
   * {@link ShardedJobSettings#getWorkerPath} to run a task.
   */
  void runTask(final ShardedJobRunId jobId, final IncrementalTaskId taskId, final int sequenceNumber);
}
