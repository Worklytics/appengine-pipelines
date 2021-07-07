package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;

/**
 * represents component that executes a Pipeline
 */
public interface PipelineRunner {

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

}
