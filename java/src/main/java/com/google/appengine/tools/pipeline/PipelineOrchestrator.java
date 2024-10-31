package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.mapreduce.*;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import lombok.NonNull;

import java.util.List;

/**
 * methods to control the execution of a pipeline; eg start/stop
 *
 *  q: TODO: rename to PipelineLauncher (analogue to Spring Batch PipelineLauncher)?
 *
 */
public interface PipelineOrchestrator {

  /**
   * Starts a {@link MapJob} with the given parameters in a new Pipeline.
   * Returns the pipeline id.
   */
  <I, O, R> JobRunId start(MapSpecification<I, O, R> specification,
                           MapSettings settings);

  /**
   * Starts a {@link MapReduceJob} with the given parameters in a new Pipeline.
   * Returns the pipeline id.
   */
  <I, K, V, O, R> JobRunId start(
    @NonNull MapReduceSpecification<I, K, V, O, R> specification, @NonNull MapReduceSettings settings);

  /**
   * Starts a new sharded job with the given ID and parameters.  The ID must
   * be unique.
   * <p>
   * This method is idempotent -- if another invocation of this method aborted
   * (or is in an unknown state, possibly still running or completed), starting
   * the job can be retried by calling the method again with the same arguments.
   * The job won't start twice unless {@link #cleanupJob} is called in between.
   *
   * @param <T> type of tasks that the job consists of
   */
  <T extends IncrementalTask> void startJob(
    ShardedJobRunId jobId,
    List<? extends T> initialTasks,
    ShardedJobController<T> controller,
    ShardedJobSettings settings);


  /**
   * Sends cancellation request to the root job.
   *
   * @param jobHandle The handle of a job
   * @throws NoSuchObjectException If a JobRecord with the given handle cannot
   *         be found in the data store.
   */
  void cancelJob(JobRunId jobHandle) throws NoSuchObjectException;

  /**
   * Changes the state of the specified job to STOPPED.
   *
   * @param jobHandle The handle of a job
   * @throws NoSuchObjectException If a JobRecord with the given handle cannot
   *         be found in the data store.
   */
  void stopJob(JobRunId jobHandle) throws NoSuchObjectException;


  /**
   * Aborts execution of the job with the given ID.  If the job has already
   * finished or does not exist, this is a no-op.
   */
  void abortJob(ShardedJobRunId jobId);

  /**
   * Deletes all data of a completed job with the given ID.
   * Data is being deleted asynchronously.
   * Returns true if job was already deleted or asynchronous task was submitted successfully.
   */
  boolean cleanupJob(ShardedJobRunId jobId);
}
