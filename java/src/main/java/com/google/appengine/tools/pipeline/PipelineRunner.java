package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobState;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.util.Pair;

import java.util.Iterator;
import java.util.Set;

/**
 * represents component that executes a Pipeline
 *
 * in theory, can be private to Pipelines? (not something people launhcing jobs actually need)
 *
 * TODO: rename to PipelineRepository (analogue to Spring Batch PipelineRepository)?
 */
public interface PipelineRunner {

  /**
   * Returns the state of the job with the given ID.  Returns null if no such
   * job exists.
   */
  ShardedJobState getJobState(ShardedJobId jobId);

  /**
   * Returns the tasks associated with this ShardedJob.
   */
  Iterator<IncrementalTaskState<IncrementalTask>> lookupTasks(ShardedJobState state);

  /**
   * @return options necessary to reconstruct this runner via PipelineRunnerFactory
   */
  PipelineBackEnd.Options getOptions();

  /**
   * @return SerializationStrategy to be used for values sent to this runner
   */
  SerializationStrategy getSerializationStrategy();

  /**
   * Creates a new JobRecord with its associated Barriers and Slots. Also
   * creates new {@link HandleSlotFilledTask} for any inputs to the Job that are
   * immediately specified. Registers all newly created objects with the
   * provided {@code UpdateSpec} for later saving.
   * <p>
   * This method is called when starting a new Pipeline, in which case it is
   * used to create the root job, and it is called from within the run() method
   * of a generator job in order to create a child job.
   *
   * @param updateSpec The {@code UpdateSpec} with which to register all newly
   *        created objects. All objects will be added to the
   *        {@link UpdateSpec#getNonTransactionalGroup() non-transaction group}
   *        of the {@code UpdateSpec}.
   * @param settings Array of {@code JobSetting} to apply to the newly created
   *        JobRecord.
   * @param generatorJob The generator job or {@code null} if we are creating
   *        the root job.
   * @param graphGUID The GUID of the child graph to which the new Job belongs
   *        or {@code null} if we are creating the root job.
   * @param jobInstance The user-supplied instance of {@code Job} that
   *        implements the Job that the newly created JobRecord represents.
   * @param params The arguments to be passed to the run() method of the newly
   *        created Job. Each argument may be an actual value or it may be an
   *        object of type {@link Value} representing either an
   *        {@link ImmediateValue} or a
   *        {@link com.google.appengine.tools.pipeline.FutureValue FutureValue}.
   *        For each element of the array, if the Object is not of type
   *        {@link Value} then it is interpreted as an {@link ImmediateValue}
   *        with the given Object as its value.
   * @return The newly constructed JobRecord.
   */
  JobRecord registerNewJobRecord(UpdateSpec updateSpec, JobSetting[] settings, Job<?> jobInstance, Object[] params);

  JobRecord registerNewJobRecord(UpdateSpec updateSpec, JobSetting[] settings,
                                        JobRecord generatorJob, String graphGUID, Job<?> jobInstance, Object[] params);

  JobRecord registerNewJobRecord(UpdateSpec updateSpec, JobRecord jobRecord,
                                        Object[] params);

  /**
   * q: move to a JobRepository or something?
   */
   Set<String> getRootPipelinesDisplayName();

  /**
   * Retrieves a JobRecord for the specified job handle. The returned instance
   * will be only partially inflated. The run and finalize barriers will not be
   * available but the output slot will be.
   *
   * @param jobHandle The handle of a job.
   * @return The corresponding JobRecord
   * @throws NoSuchObjectException If a JobRecord with the given handle cannot
   *         be found in the data store.
   */
   JobRecord getJob(JobId jobHandle) throws NoSuchObjectException;

  /**
   * Returns all the associated PipelineModelObject for a root pipeline.
   *
   * @throws IllegalArgumentException if root pipeline was not found.
   */
   PipelineObjects queryFullPipeline(JobId rootJobHandle);

   Pair<? extends Iterable<JobRecord>, String> queryRootPipelines(String classFilter, String cursor, int limit);

  /**
   * Delete all data store entities corresponding to the given pipeline.
   *
   * @param pipelineHandle The handle of the pipeline to be deleted
   * @param force          If this parameter is not {@code true} then this method will
   *                       throw an {@link IllegalStateException} if the specified pipeline is
   *                       not in the {@link JobRecord.State#FINALIZED} or {@link JobRecord.State#STOPPED} state.
   * @throws NoSuchObjectException If there is no Job with the given key.
   * @throws IllegalStateException If {@code force = false} and the specified
   *                               pipeline is not in the {@link JobRecord.State#FINALIZED} or
   *                               {@link JobRecord.State#STOPPED} state.
   */
   void deletePipelineRecords(JobId pipelineHandle, boolean force)
    throws NoSuchObjectException, IllegalStateException;


  /**
   * Process an incoming task received from the App Engine task queue.
   *
   * @param task The task to be processed.
   *
   */
   void processTask(Task task);
}
