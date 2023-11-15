package com.google.appengine.tools.pipeline;

/**
 * methods to control the execution of a pipeline.
 */
public interface PipelineOrchestrator {

  /**
   * Sends cancellation request to the root job.
   *
   * @param jobHandle The handle of a job
   * @throws NoSuchObjectException If a JobRecord with the given handle cannot
   *         be found in the data store.
   */
  void cancelJob(String jobHandle) throws NoSuchObjectException;

  /**
   * Changes the state of the specified job to STOPPED.
   *
   * @param jobHandle The handle of a job
   * @throws NoSuchObjectException If a JobRecord with the given handle cannot
   *         be found in the data store.
   */
  void stopJob(String jobHandle) throws NoSuchObjectException;
}
