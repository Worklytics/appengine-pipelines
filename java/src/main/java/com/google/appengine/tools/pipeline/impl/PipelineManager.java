// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.tools.mapreduce.*;
import com.google.appengine.tools.mapreduce.impl.shardedjob.*;
import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.di.DaggerMultiTenantComponent;
import com.google.appengine.tools.pipeline.di.MultiTenantComponent;
import com.google.appengine.tools.pipeline.di.TenantModule;
import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.appengine.tools.pipeline.impl.util.DIUtil;
import com.google.cloud.datastore.Key;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec.Group;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord.InflationType;
import com.google.appengine.tools.pipeline.impl.model.JobRecord.State;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.model.SlotDescriptor;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.appengine.tools.pipeline.impl.tasks.CancelJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.DelayedSlotFillTask;
import com.google.appengine.tools.pipeline.impl.tasks.DeletePipelineTask;
import com.google.appengine.tools.pipeline.impl.tasks.FinalizeJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleChildExceptionTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleSlotFilledTask;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.appengine.tools.pipeline.util.Pair;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.inject.Provider;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * The central hub of the Pipeline implementation.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 * TODO: this is implementation; NOT really part of public API of pkg, although we depend on it somewhat.
 */
@AllArgsConstructor(onConstructor_ = @Inject)
@Log
public class PipelineManager implements PipelineRunner, PipelineOrchestrator {

  public static final String DEFAULT_QUEUE_NAME = "default";

  final Provider<PipelineService> pipelineServiceProvider;
  final ShardedJobRunner shardedJobRunner;
  final PipelineBackEnd backEnd;

  public static PipelineManager getInstance(AppEngineBackEnd.Options options) {
    MultiTenantComponent multiTenantComponent =  DaggerMultiTenantComponent.create();
    return multiTenantComponent.clientComponent(new TenantModule(options)).pipelineManager();
  }

  public static PipelineManager getInstance() {
    return getInstance(AppEngineBackEnd.Options.defaults());
  }

  PipelineBackEnd.Options getBackendOptions() {
    return backEnd.getOptions();
  }

  /**
   * Creates and launches a new Pipeline
   * <p>
   * Creates the root Job with its associated Barriers and Slots and saves them
   * to the data store. All slots in the root job are immediately filled with
   * the values of the parameters to this method, and
   * {@link HandleSlotFilledTask HandleSlotFilledTasks} are enqueued for each of
   * the filled slots.
   *
   * @param settings JobSetting array used to control details of the Pipeline
   * @param jobInstance A user-supplied instance of {@link Job} that will serve
   *        as the root job of the Pipeline.
   * @param params Arguments to the root job's run() method
   * @return The pipelineID of the newly created pipeline, also known as the
   *         rootJobID.
   */
  public JobRunId startNewPipeline(
      JobSetting[] settings, Job<?> jobInstance, Object... params) {
    UpdateSpec updateSpec = new UpdateSpec(null);
    Job<?> rootJobInstance = jobInstance;
    jobInstance.setPipelineBackendOptions(this.getBackendOptions());
    // If rootJobInstance has exceptionHandler it has to be wrapped to ensure that root job
    // ends up in finalized state in case of exception in run method and
    // exceptionHandler returning a result.
    if (JobRecord.isExceptionHandlerSpecified(jobInstance)) {
      rootJobInstance = new RootJobInstance(jobInstance, settings, params);
      params = new Object[0];
    }
    // Create the root Job and its associated Barriers and Slots
    // Passing null for parent JobRecord and graphGUID
    // Create HandleSlotFilledTasks for the input parameters.
    JobRecord jobRecord = registerNewJobRecord(updateSpec, settings, rootJobInstance, params);
    updateSpec.setRootJobKey(jobRecord.getRootJobKey());
    // Save the Pipeline model objects and enqueue the tasks that start the Pipeline executing.
    backEnd.save(updateSpec);
    return JobRunId.of(jobRecord.getKey());
  }

  @Override
  public <T extends IncrementalTask> void startJob(
    ShardedJobRunId jobId,
    List<? extends T> initialTasks,
    ShardedJobController<T> controller,
    ShardedJobSettings settings) {
    shardedJobRunner.startJob(jobId, initialTasks, controller, settings);
  }

  @Override
  public void abortJob(ShardedJobRunId jobId) {
    shardedJobRunner.abortJob(jobId);
  }

  @Override
  public boolean cleanupJob(ShardedJobRunId jobId) {
    return shardedJobRunner.cleanupJob(jobId);
  }

  @Override
  public void deletePipelineAsync(@NonNull JobRunId pipelineRunId, @NonNull Long delayMillis) {
    Key key = JobRecord.keyFromPipelineHandle(pipelineRunId);
    DeletePipelineTask deletePipelineTask = new DeletePipelineTask(key, false, QueueSettings.builder().build());
    deletePipelineTask.getQueueSettings().setDelayInSeconds(delayMillis / 1000);

    //q: add retries? old one had 5 with 20s backoff; but tasks are auto-retried, right?

    backEnd.enqueue(deletePipelineTask);
  }

  @Override
  public JobRecord registerNewJobRecord(UpdateSpec updateSpec, JobSetting[] settings, Job<?> jobInstance, Object[] params) {

    //TODO: fix this dependency; JobRecord implicitly depends on backend having notion of 'project'
    // (and indeed, is coupled to Cloud Datastore underneath)
    // --> JobRecordFactory or something, that gets injected
    String projectId = backEnd.getOptions().as(AppEngineBackEnd.Options.class).getProjectId();

    if (projectId.equals("no_app_id")) {
      throw new IllegalStateException("projectId is 'no_app_id'; this isn't legal GCP project id");
    }

    JobRecord jobRecord = JobRecord.createRootJobRecord(projectId, jobInstance, getSerializationStrategy(), settings);
    pinServiceAndVersion(jobRecord, settings);

    return registerNewJobRecord(updateSpec, jobRecord, params);
  }

  @VisibleForTesting
  void pinServiceAndVersion(JobRecord jobRecord, JobSetting[] settings) {

    //pin service
    PipelineService pipelineService = pipelineServiceProvider.get();
    String service = jobRecord.getQueueSettings().getOnService();
    if (service == null) {
      Optional<String> serviceSetting = Arrays.stream(settings)
        .filter(setting -> setting instanceof JobSetting.OnService)
        .map(setting -> ((JobSetting.OnService) setting).getValue())
        .findFirst();
      service = serviceSetting.orElseGet(pipelineService::getDefaultWorkerService);
      jobRecord.getQueueSettings().setOnService(service);
    }

    // pin service version
    if (jobRecord.getQueueSettings().getOnServiceVersion() == null) {
      String currentVersion = pipelineService.getCurrentVersion(service);
      Optional<String> versionSetting = Arrays.stream(settings).filter(setting -> setting instanceof JobSetting.OnServiceVersion)
        .findFirst()
        .map(setting -> ((JobSetting.OnServiceVersion) setting).getValue());

      String targetVersion = versionSetting.orElse(currentVersion);
      jobRecord.getQueueSettings().setOnServiceVersion(targetVersion);
    }
  }

  @Override
  public JobRecord registerNewJobRecord(UpdateSpec updateSpec, JobSetting[] settings,
                                        JobRecord generatorJob, String graphGUID, Job<?> jobInstance, Object[] params) {
    JobRecord jobRecord = new JobRecord(generatorJob, graphGUID, jobInstance, false, settings, getSerializationStrategy());

    //shouldn't be needed, as should have copied queue settings from generatorJob
    pinServiceAndVersion(jobRecord, settings);

    return registerNewJobRecord(updateSpec, jobRecord, params);
  }

  @Override
  public JobRecord registerNewJobRecord(UpdateSpec updateSpec, JobRecord jobRecord,
      Object[] params) {
    if (log.isLoggable(Level.FINE)) {
      log.fine("registerNewJobRecord job(\"" + jobRecord + "\")");
    }
    updateSpec.setRootJobKey(jobRecord.getRootJobKey());

    Key generatorKey = jobRecord.getGeneratorJobKey();
    // Add slots to the RunBarrier corresponding to the input parameters
    String graphGuid = jobRecord.getGraphGUID();
    for (Object param : params) {
      Value<?> value;
      if (null != param && param instanceof Value<?>) {
        value = (Value<?>) param;
      } else {
        value = new ImmediateValue<>(param);
      }
      registerSlotsWithBarrier(updateSpec, value, jobRecord.getRootJobKey(), generatorKey,
          jobRecord.getQueueSettings(), graphGuid, jobRecord.getRunBarrierInflated());
    }

    if (0 == jobRecord.getRunBarrierInflated().getWaitingOnKeys().size()) {
      // If the run barrier is not waiting on anything, add a phantom filled
      // slot in order to trigger a HandleSlotFilledTask in order to trigger
      // a RunJobTask.
      Slot slot = new Slot(jobRecord.getRootJobKey(), generatorKey, graphGuid, getSerializationStrategy());
      jobRecord.getRunBarrierInflated().addPhantomArgumentSlot(slot);
      registerSlotFilled(updateSpec, jobRecord.getQueueSettings(), slot, null);
    }

    // Register the newly created objects with the UpdateSpec.
    // The slots in the run Barrier have already been registered
    // and the finalize Barrier doesn't have any slots yet.
    // Any HandleSlotFilledTasks have also been registered already.
    Group updateGroup = updateSpec.getNonTransactionalGroup();
    updateGroup.includeBarrier(jobRecord.getRunBarrierInflated());
    updateGroup.includeBarrier(jobRecord.getFinalizeBarrierInflated());
    updateGroup.includeSlot(jobRecord.getOutputSlotInflated());
    updateGroup.includeJob(jobRecord);
    updateGroup.includeJobInstanceRecord(jobRecord.getJobInstanceInflated());

    return jobRecord;
  }

  @Override
  public ShardedJobState getJobState(ShardedJobRunId jobId) {
    return shardedJobRunner.getJobState(jobId);
  }

  @Override
  public List<IncrementalTaskState<IncrementalTask>> lookupTasks(ShardedJobState state) {
    return shardedJobRunner.lookupTasks(state.getShardedJobId(), state.getTotalTaskCount(), true);
  }


  /**
   * Given a {@code Value} and a {@code Barrier}, we add one or more slots to
   * the waitingFor list of the barrier corresponding to the {@code Value}. We
   * also create {@link HandleSlotFilledTask HandleSlotFilledTasks} for all
   * filled slots. We register all newly created Slots and Tasks with the given
   * {@code UpdateSpec}.
   * <p>
   * If the value is an {@code ImmediateValue} we make a new slot, register it
   * as filled and add it. If the value is a {@code FutureValue} we add the slot
   * wrapped by the {@code FutreValueImpl}. If the value is a {@code FutureList}
   * then we add multiple slots, one for each {@code Value} in the List wrapped
   * by the {@code FutureList}. This process is not recursive because we do not
   * currently support {@code FutureLists} of {@code FutureLists}.
   *
   * @param updateSpec All newly created Slots will be added to the
   *        {@link UpdateSpec#getNonTransactionalGroup() non-transactional
   *        group} of the updateSpec. All {@link HandleSlotFilledTask
   *        HandleSlotFilledTasks} created will be added to the
   *        {@link UpdateSpec#getFinalTransaction() final transaction}. Note
   *        that {@code barrier} will not be added to updateSpec. That must be
   *        done by the caller.
   * @param value A {@code Value}. {@code Null} is interpreted as an
   *        {@code ImmediateValue} with a value of {@code Null}.
   * @param rootJobKey The rootJobKey of the Pipeline in which the given Barrier
   *        lives.
   * @param generatorJobKey The key of the generator Job of the local graph in
   *        which the given barrier lives, or {@code null} if the barrier lives
   *        in the root Job graph.
   * @param queueSettings The QueueSettings for tasks created by this method
   * @param graphGUID The GUID of the local graph in which the barrier lives, or
   *        {@code null} if the barrier lives in the root Job graph.
   * @param barrier The barrier to which we will add the slots
   */
  private void registerSlotsWithBarrier(UpdateSpec updateSpec, Value<?> value,
      Key rootJobKey, Key generatorJobKey, QueueSettings queueSettings, String graphGUID,
      Barrier barrier) {
    if (null == value || value instanceof ImmediateValue<?>) {
      Object concreteValue = null;
      if (null != value) {
        ImmediateValue<?> iv = (ImmediateValue<?>) value;
        concreteValue = iv.getValue();
      }
      Slot slot = new Slot(rootJobKey, generatorJobKey, graphGUID, backEnd.getSerializationStrategy());
      registerSlotFilled(updateSpec, queueSettings, slot, concreteValue);
      if (barrier.getType() == Barrier.Type.FINALIZE && !barrier.getWaitingOnKeys().isEmpty()) {
        log.log(Level.WARNING, "Adding multiple slots to finalize barrier? Job key: {0}", rootJobKey.toString());
      }
      barrier.addRegularArgumentSlot(slot);
    } else if (value instanceof FutureValueImpl<?>) {
      FutureValueImpl<?> futureValue = (FutureValueImpl<?>) value;
      Slot slot = futureValue.getSlot();
      if (barrier.getType() == Barrier.Type.FINALIZE && !barrier.getWaitingOnKeys().isEmpty()) {
        log.log(Level.WARNING, "Adding multiple slots to finalize barrier? Job key: {0}", rootJobKey.toString());
      }
      barrier.addRegularArgumentSlot(slot);
      updateSpec.getNonTransactionalGroup().includeSlot(slot);
    } else if (value instanceof FutureList<?>) {
      FutureList<?> futureList = (FutureList<?>) value;
      List<Slot> slotList = new ArrayList<>(futureList.getListOfValues().size());
      // The dummyListSlot is a marker slot that indicates that the
      // next group of slots forms a single list argument.
      Slot dummyListSlot = new Slot(rootJobKey, generatorJobKey, graphGUID, backEnd.getSerializationStrategy());
      registerSlotFilled(updateSpec, queueSettings, dummyListSlot, null);
      for (Value<?> valFromList : futureList.getListOfValues()) {
        Slot slot = null;
        if (valFromList instanceof ImmediateValue<?>) {
          ImmediateValue<?> ivFromList = (ImmediateValue<?>) valFromList;
          slot = new Slot(rootJobKey, generatorJobKey, graphGUID, backEnd.getSerializationStrategy());
          registerSlotFilled(updateSpec, queueSettings, slot, ivFromList.getValue());
        } else if (valFromList instanceof FutureValueImpl<?>) {
          FutureValueImpl<?> futureValFromList = (FutureValueImpl<?>) valFromList;
          slot = futureValFromList.getSlot();
        } else if (value instanceof FutureList<?>) {
          throw new IllegalArgumentException(
              "The Pipeline framework does not currently support FutureLists of FutureLists");
        } else {
          throwUnrecognizedValueException(valFromList);
        }
        slotList.add(slot);
        updateSpec.getNonTransactionalGroup().includeSlot(slot);
      }
      barrier.addListArgumentSlots(dummyListSlot, slotList);
    } else {
      throwUnrecognizedValueException(value);
    }
  }

  private void throwUnrecognizedValueException(Value<?> value) {
    throw new RuntimeException(
        "Internal logic error: Unrecognized implementation of Value interface: "
        + value.getClass().getName());
  }

  /**
   * Given a Slot and a concrete value with which to fill the slot, we fill the
   * value with the slot and create a new {@link HandleSlotFilledTask} for the
   * newly filled Slot. We register the Slot and the Task with the given
   * UpdateSpec for later saving.
   *
   * @param updateSpec The Slot will be added to the
   *        {@link UpdateSpec#getNonTransactionalGroup() non-transactional
   *        group} of the updateSpec. The new {@link HandleSlotFilledTask} will
   *        be added to the {@link UpdateSpec#getFinalTransaction() final
   *        transaction}.
   * @param queueSettings queue settings for the created task
   * @param slot the Slot to fill
   * @param value the value with which to fill it
   */
  private static void registerSlotFilled(
      UpdateSpec updateSpec, QueueSettings queueSettings, Slot slot, Object value) {
    slot.fill(value);
    updateSpec.getNonTransactionalGroup().includeSlot(slot);
    PipelineTask pipelineTask = new HandleSlotFilledTask(slot.getKey(), queueSettings);
    updateSpec.getFinalTransaction().registerTask(pipelineTask);
  }

  @Override
  public PipelineObjects queryFullPipeline(JobRunId rootJobHandle) {
    return backEnd.queryFullPipeline(JobRecord.keyFromPipelineHandle(rootJobHandle));
  }

  @Override
  public Pair<? extends Iterable<JobRecord>, String> queryRootPipelines(
      String classFilter, String cursor, int limit) {
    return backEnd.queryRootPipelines(classFilter, cursor, limit);
  }

  public Set<String> getRootPipelinesDisplayName() {
    return backEnd.getRootPipelinesDisplayName();
  }

  @Override
  public JobRecord getJob(@NonNull JobRunId jobHandle) throws NoSuchObjectException {
    log.finest("getJob: " + jobHandle);
    return backEnd.queryJob(JobRecord.keyFromPipelineHandle(jobHandle), InflationType.FOR_OUTPUT);
  }

  public void stopJob(@NonNull JobRunId jobHandle) throws NoSuchObjectException {
    JobRecord jobRecord = backEnd.queryJob(JobRecord.keyFromPipelineHandle(jobHandle), InflationType.NONE);
    jobRecord.setState(State.STOPPED);
    UpdateSpec updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    updateSpec.getOrCreateTransaction("stopJob").includeJob(jobRecord);
    backEnd.save(updateSpec);
  }

  @Override
  public <I, O, R> JobRunId start(MapSpecification<I, O, R> specification, MapSettings settings) {
    if (settings.getWorkerQueueName() == null) {
      settings = settings.toBuilder().workerQueueName(DEFAULT_QUEUE_NAME).build();
    }
    return startNewPipeline(settings.toJobSettings(), new MapJob<>(specification, settings));
  }

  @Override
  public <I, K, V, O, R> JobRunId start(@NonNull MapReduceSpecification<I, K, V, O, R> specification,
                                        @NonNull MapReduceSettings settings) {
    if (settings.getWorkerQueueName() == null) {
      settings =  settings.toBuilder().workerQueueName(DEFAULT_QUEUE_NAME).build();
    }
    return startNewPipeline(settings.toJobSettings(), new MapReduceJob<>(specification, settings));
  }



  public void cancelJob(@NonNull JobRunId jobHandle) throws NoSuchObjectException {
    Key key = JobRecord.keyFromPipelineHandle(jobHandle);
    JobRecord jobRecord = backEnd.queryJob(key, InflationType.NONE);
    CancelJobTask cancelJobTask = new CancelJobTask(key, jobRecord.getQueueSettings());
    backEnd.enqueue(cancelJobTask);
  }

  /**
   * Delete all data store entities corresponding to the given pipeline.
   *
   * @param pipelineHandle The handle of the pipeline to be deleted
   * @param force          If this parameter is not {@code true} then this method will
   *                       throw an {@link IllegalStateException} if the specified pipeline is
   *                       not in the {@link State#FINALIZED} or {@link State#STOPPED} state.
   * @throws NoSuchObjectException If there is no Job with the given key.
   * @throws IllegalStateException If {@code force = false} and the specified
   *                               pipeline is not in the {@link State#FINALIZED} or
   *                               {@link State#STOPPED} state.
   */
  public void deletePipelineRecords(@NonNull JobRunId pipelineHandle, boolean force)
      throws NoSuchObjectException, IllegalStateException {
    log.info("pipelineHandle: " + pipelineHandle + ", force: " + force);
    backEnd.deletePipeline(pipelineHandle, force);
  }

  /**
   * just implementation of {@link PipelineService#submitPromisedValue(SlotId, Object)}, not really sure why it's here
   *
   * @param promiseHandle id of slot to fill for promise
   * @param value to fill slot with
   * @throws NoSuchObjectException
   * @throws OrphanedObjectException
   */
  void submitPromisedValue(@NonNull SlotId promiseHandle, Object value)
      throws NoSuchObjectException, OrphanedObjectException {
    Key key = Slot.keyFromHandle(promiseHandle);
    Slot slot = null;
    // It is possible, though unlikely, that we might be asked to accept a
    // promise before the slot to hold the promise has been saved. We will try 5
    // times, sleeping 1, 2, 4, 8 seconds between attempts.
    int attempts = 0;

    while (slot == null) {
      attempts++;
      try {
        slot = backEnd.querySlot(key, false);
      } catch (NoSuchObjectException e) {
        if (attempts >= 5) {
          throw new NoSuchObjectException("There is no promise with handle " + promiseHandle);
        }
        Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds((long) Math.pow(2.0, attempts - 1)));
      }
    }

    Key generatorJobKey = slot.getGeneratorJobKey();
    if (null == generatorJobKey) {
      throw new RuntimeException(
          "Pipeline is fatally corrupted. Slot for promised value has no generatorJobKey: " + slot);
    }
    JobRecord generatorJob = backEnd.queryJob(generatorJobKey, InflationType.NONE);
    if (null == generatorJob) {
      throw new RuntimeException("Pipeline is fatally corrupted. "
          + "The generator job for a promised value slot was not found: " + generatorJobKey);
    }
    String childGraphGuid = generatorJob.getChildGraphGuid();
    if (null == childGraphGuid) {
      // The generator job has not been saved with a childGraphGuid yet. This can happen if the
      // promise handle leaked out to an external thread before the job that generated it
      // had finished.
      throw new NoSuchObjectException(
          "The framework is not ready to accept the promised value yet. "
          + "Please try again after the job that generated the promis handle has completed.");
    }
    if (!childGraphGuid.equals(slot.getGraphGUID())) {
      // The slot has been orphaned
      throw new OrphanedObjectException(promiseHandle);
    }
    UpdateSpec updateSpec = new UpdateSpec(slot.getRootJobKey());
    registerSlotFilled(updateSpec, generatorJob.getQueueSettings(), slot, value);
    backEnd.save(updateSpec);
  }

  @Override
  public PipelineBackEnd.Options getOptions() {
    return getBackendOptions();
  }

  public SerializationStrategy getSerializationStrategy() {
    return backEnd.getSerializationStrategy();
  }

  /**
   * The root job instance used to wrap a user provided root if the user
   * provided root job has exceptionHandler specified.
   */
  private static final class RootJobInstance extends Job0<Object> {

    private static final long serialVersionUID = -2162670129577469245L;

    private final Job<?> jobInstance;
    private final JobSetting[] settings;
    private final Object[] params;

    public RootJobInstance(Job<?> jobInstance, JobSetting[] settings, Object[] params) {
      this.jobInstance = jobInstance;
      this.settings = settings;
      this.params = params;
    }

    @Override
    public Value<Object> run() throws Exception {
      return futureCallUnchecked(settings, jobInstance, params);
    }

    @Override
    public String getJobDisplayName() {
      return jobInstance.getJobDisplayName();
    }
  }

  /**
   * A RuntimeException which, when thrown, causes us to abandon the current
   * task, by returning a 200.
   */
  private class AbandonTaskException extends RuntimeException {
    private static final long serialVersionUID = 358437646006972459L;
  }

  /**
   * Process an incoming task received from the App Engine task queue.
   *
   * @param pipelineTask The task to be processed.
   *
   *
   */
  public void processTask(PipelineTask pipelineTask) {
    log.finest("Processing task " + pipelineTask);
    try {
      switch (pipelineTask.getType()) {
        case RUN_JOB:
          runJob((RunJobTask) pipelineTask);
          break;
        case HANDLE_SLOT_FILLED:
          handleSlotFilled((HandleSlotFilledTask) pipelineTask);
          break;
        case FINALIZE_JOB:
          finalizeJob((FinalizeJobTask) pipelineTask);
          break;
        case CANCEL_JOB:
          cancelJob((CancelJobTask) pipelineTask);
          break;
        case DELETE_PIPELINE:
          DeletePipelineTask deletePipelineTask = (DeletePipelineTask) pipelineTask;
          try {
            backEnd.deletePipeline(JobRunId.of(deletePipelineTask.getRootJobKey()), deletePipelineTask.shouldForce());
          } catch (Exception e) {
            log.log(Level.WARNING, "DeletePipeline operation failed.", e);
          }
          break;
        case HANDLE_CHILD_EXCEPTION:
          handleChildException((HandleChildExceptionTask) pipelineTask);
          break;
        case DELAYED_SLOT_FILL:
          handleDelayedSlotFill((DelayedSlotFillTask) pipelineTask);
          break;
        default:
          throw new IllegalArgumentException("Unrecognized task type: " + pipelineTask.getType());
      }
    } catch (AbandonTaskException ate) {
      // return 200;
    }
  }

  //TODO: can we implement this in a better way with modern Java?
  private void invokePrivateJobMethod(String methodName, Job<?> job, Object... params) {
    Class<?>[] signature = new Class<?>[params.length];
    int i = 0;
    for (Object param : params) {
      signature[i++] = param.getClass();
    }
    invokePrivateJobMethod(methodName, job, signature, params);
  }

  private void invokePrivateJobMethod(
      String methodName, Job<?> job, Class<?>[] signature, Object... params) {
    try {
      Method method = Job.class.getDeclaredMethod(methodName, signature);
      method.setAccessible(true);
      method.invoke(job, params);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return handleException method (if callErrorHandler is <code>true</code>).
   * Otherwise return first run method (order is defined by order of
   * {@link Class#getMethods()} is return. <br>
   * TODO(user) Consider actually looking for a method with matching
   * signature<br>
   * TODO(maximf) Consider getting rid of reflection based invocations
   * completely.
   *
   * @param klass Class of the user provided job
   * @param callErrorHandler should handleException method be returned instead
   *        of run
   * @param params parameters to be passed to the method to invoke
   * @return Either run or handleException method of {@code class}. <code>null
   *         </code> is returned if @{code callErrorHandler} is <code>true</code>
   *         and no handleException with matching signature is found.
   *
   */
  @SuppressWarnings("unchecked")
  private Method findJobMethodToInvoke(
      Class<?> klass, boolean callErrorHandler, Object[] params) {
    Method runMethod = null;
    if (callErrorHandler) {
      if (params.length != 1) {
        throw new RuntimeException(
            "Invalid number of parameters passed to handleException:" + params.length);
      }
      Object parameter = params[0];
      if (parameter == null) {
        throw new RuntimeException("Null parameters passed to handleException:" + params.length);
      }
      Class<? extends Object> parameterClass = parameter.getClass();
      if (!Throwable.class.isAssignableFrom(parameterClass)) {
        throw new RuntimeException("Parameter that is not an exception passed to handleException:"
            + parameterClass.getName());
      }
      Class<? extends Throwable> exceptionClass = (Class<? extends Throwable>) parameterClass;
      do {
        try {
          runMethod = klass.getMethod(
              JobRecord.EXCEPTION_HANDLER_METHOD_NAME, new Class<?>[] {exceptionClass});
          break;
        } catch (NoSuchMethodException e) {
          // Ignore, try parent instead.
        }
        exceptionClass = (Class<? extends Throwable>) exceptionClass.getSuperclass();
      } while (exceptionClass != null);
    } else {
      for (Method method : klass.getMethods()) {
        if ("run".equals(method.getName())) {
          runMethod = method;
          break;
        }
      }
    }
    return runMethod;
  }

  private void setJobRecord(Job<?> job, JobRecord jobRecord) {
    invokePrivateJobMethod("setJobRecord", job, jobRecord);
  }

  private void setCurrentRunGuid(Job<?> job, String guid) {
    invokePrivateJobMethod("setCurrentRunGuid", job, guid);
  }

  private void setUpdateSpec(Job<?> job, UpdateSpec updateSpec) {
    invokePrivateJobMethod("setUpdateSpec", job, updateSpec);
  }


  /**
   * Run the job with the given key.
   * <p>
   * We fetch the {@link JobRecord} from the data store and then fetch its run
   * {@link Barrier} and all of the {@link Slot Slots} in the run
   * {@code Barrier} (which should be filled.) We use the values of the filled
   * {@code Slots} to populate the arguments of the run() method of the
   * {@link Job} instance associated with the job and we invoke the
   * {@code run()} method. We save any generated child job graph and we enqueue
   * a {@link HandleSlotFilledTask} for any slots that are filled immediately.
   *
   * @see "http://goto/java-pipeline-model"
   */
  private void runJob(RunJobTask task) {
    Key jobKey = task.getJobKey();
    JobRecord jobRecord = queryJobOrAbandonTask(jobKey, InflationType.FOR_RUN);
    jobRecord.getQueueSettings().merge(task.getQueueSettings());
    Key rootJobKey = jobRecord.getRootJobKey();
    log.info("Running pipeline job " + jobKey.getName() + "; UI at "
        + PipelineServlet.makeViewerUrl(jobRecord.getPipelineRunId(), jobRecord.getJobRunId()));
    JobRecord rootJobRecord;
    if (rootJobKey.equals(jobKey)) {
      rootJobRecord = jobRecord;
    } else {
      rootJobRecord = queryJobOrAbandonTask(rootJobKey, InflationType.NONE);
    }
    if (rootJobRecord.getState() == State.STOPPED) {
      log.warning("The pipeline has been stopped: " + rootJobRecord);
      throw new AbandonTaskException();
    }

    // TODO(user): b/12301978, check if its a previous run and if so AbandonTaskException
    Barrier runBarrier = jobRecord.getRunBarrierInflated();
    if (null == runBarrier) {
      throw new RuntimeException("Internal logic error: " + jobRecord + " has not been inflated.");
    }
    Barrier finalizeBarrier = jobRecord.getFinalizeBarrierInflated();
    if (null == finalizeBarrier) {
      throw new RuntimeException(
          "Internal logic error: finalize barrier not inflated in " + jobRecord);
    }

    // Release the run barrier now so any concurrent HandleSlotFilled tasks
    // will stop trying to release it.
    runBarrier.setReleased();
    UpdateSpec tempSpec = new UpdateSpec(rootJobKey);
    tempSpec.getOrCreateTransaction("releaseRunBarrier").includeBarrier(runBarrier);
    backEnd.save(tempSpec);

    State jobState = jobRecord.getState();
    switch (jobState) {
      case WAITING_TO_RUN:
      case RETRY:
        // OK, proceed
        break;
      case WAITING_TO_FINALIZE:
        log.info("This job has already been run " + jobRecord);
        return;
      case STOPPED:
        log.info("This job has been stoped " + jobRecord);
        return;
      case CANCELED:
        log.info("This job has already been canceled " + jobRecord);
        return;
      case FINALIZED:
        log.info("This job has already been run " + jobRecord);
        return;
    }

    // Deserialize the instance of Job and set some values on the instance
    Job<?> job = restore(jobRecord);
    inject(job);


    // Get the run() method we will invoke and its arguments
    Object[] params = runBarrier.buildArgumentArray();
    boolean callExceptionHandler = jobRecord.isCallExceptionHandler();
    Method methodToExecute =
        findJobMethodToInvoke(job.getClass(), callExceptionHandler, params);
    if (callExceptionHandler && methodToExecute == null) {
      // No matching exceptionHandler found. Propagate to the parent.
      Throwable exceptionToHandle = (Throwable) params[0];
      handleExceptionDuringRun(jobRecord, rootJobRecord, job.getCurrentRunGUID(), exceptionToHandle);
      return;
    }
    if (log.isLoggable(Level.FINEST)) {
      StringBuilder builder = new StringBuilder(1024);
      builder.append("Running " + jobRecord + " with params: ");
      builder.append(StringUtils.toString(params));
      log.finest(builder.toString());
    }

    // Set the Job's start time and save the jobRecord now before we invoke
    // run(). The start time will be displayed in the UI.
    jobRecord.incrementAttemptNumber();
    jobRecord.setStartTime(Instant.now());
    tempSpec = new UpdateSpec(jobRecord.getRootJobKey());
    tempSpec.getNonTransactionalGroup().includeJob(jobRecord);
    if (!backEnd.saveWithJobStateCheck(
        tempSpec, jobKey, State.WAITING_TO_RUN, State.RETRY)) {
      log.info("Ignoring runJob request for job " + jobRecord + " which is not in a"
          + " WAITING_TO_RUN or a RETRY state");
      return;
    }
    //TODO(user): Use the commented code to avoid resetting stack trace of rethrown exceptions
    //    List<Throwable> throwableParams = new ArrayList<Throwable>(params.length);
    //    for (Object param : params) {
    //      if (param instanceof Throwable) {
    //        throwableParams.add((Throwable) param);
    //      }
    //    }
    // Invoke the run or handleException method. This has the side-effect of populating
    // the UpdateSpec with any child job graph generated by the invoked method.
    Value<?> returnValue = null;
    Throwable caughtException = null;
    try {
      methodToExecute.setAccessible(true);
      returnValue = (Value<?>) methodToExecute.invoke(job, params);
    } catch (InvocationTargetException e) {
      caughtException = e.getCause();
    } catch (Throwable e) {
      caughtException = e;
    }
    if (null != caughtException) {
      //TODO(user): use the following condition to keep original exception trace
      //      if (!throwableParams.contains(caughtException)) {
      //      }
      handleExceptionDuringRun(jobRecord, rootJobRecord, job.getCurrentRunGUID(), caughtException);
      return;
    }

    // The run() method returned without error.
    // We do all of the following in a transaction:
    // (1) Check that the job is currently in the state WAITING_TO_RUN or RETRY
    // (2) Change the state of the job to WAITING_TO_FINALIZE
    // (3) Set the finalize slot to be the one generated by the run() method
    // (4) Set the job's child graph GUID to be the currentRunGUID
    // (5) Enqueue a FanoutTask that will fan-out to a set of
    // HandleSlotFilledTasks for each of the slots that were immediately filled
    // by the running of the job.
    // See "http://goto/java-pipeline-model".
    log.finest("Job returned: " + returnValue);
    registerSlotsWithBarrier(job.getUpdateSpec(), returnValue, rootJobKey, jobRecord.getKey(),
        jobRecord.getQueueSettings(), job.getCurrentRunGUID(), finalizeBarrier);
    jobRecord.setState(State.WAITING_TO_FINALIZE);
    jobRecord.setChildGraphGuid(job.getCurrentRunGUID());
    job.getUpdateSpec().getFinalTransaction().includeJob(jobRecord);
    job.getUpdateSpec().getFinalTransaction().includeBarrier(finalizeBarrier);
    backEnd.saveWithJobStateCheck(
        job.getUpdateSpec(), jobKey, State.WAITING_TO_RUN, State.RETRY);
  }

  private void inject(Job<?> job) {
    if (job instanceof RootJobInstance) {
      inject(((RootJobInstance) job).jobInstance);
    }

    if (DIUtil.isInjectable(job)) {
      DIUtil.inject(job);
    }
  }

  /**
   * deserializes the Job instance corresponding to the given JobRecord.
   * @param jobRecord of execution of Job
   * @return a Job instance, with runtime values set
   */
  Job<?> restore(JobRecord jobRecord) {
    JobInstanceRecord jobExecutionRecord = jobRecord.getJobInstanceInflated();

    if (jobExecutionRecord == null) {
      throw new RuntimeException(
        "Internal logic error:" + jobRecord + " does not have jobInstanceInflated.");
    }

    Job<?> job = jobExecutionRecord.getJobInstanceDeserialized();
    invokePrivateJobMethod("setPipelineManager", job, this);
    invokePrivateJobMethod("setPipelineService", job, pipelineServiceProvider.get());
    invokePrivateJobMethod("setShardedJobRunner", job, shardedJobRunner);
    UpdateSpec updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    setJobRecord(job, jobRecord);
    String currentRunGUID = GUIDGenerator.nextGUID();
    setCurrentRunGuid(job, currentRunGUID);
    setUpdateSpec(job, updateSpec);

    return job;
  }

  private void cancelJob(CancelJobTask cancelJobTask) {
    Key jobKey = cancelJobTask.getJobKey();
    JobRecord jobRecord = queryJobOrAbandonTask(jobKey, InflationType.FOR_RUN);
    jobRecord.getQueueSettings().merge(cancelJobTask.getQueueSettings());
    Key rootJobKey = jobRecord.getRootJobKey();
    log.info("Cancelling pipeline job " + jobKey.getName());
    JobRecord rootJobRecord;
    if (rootJobKey.equals(jobKey)) {
      rootJobRecord = jobRecord;
    } else {
      rootJobRecord = queryJobOrAbandonTask(rootJobKey, InflationType.NONE);
    }
    if (rootJobRecord.getState() == State.STOPPED) {
      log.warning("The pipeline has been stopped: " + rootJobRecord);
      throw new AbandonTaskException();
    }

    switch (jobRecord.getState()) {
      case WAITING_TO_RUN:
      case RETRY:
      case WAITING_TO_FINALIZE:
        // OK, proceed
        break;
      case STOPPED:
        log.info("This job has been stoped " + jobRecord);
        return;
      case CANCELED:
        log.info("This job has already been canceled " + jobRecord);
        return;
      case FINALIZED:
        log.info("This job has already been run " + jobRecord);
        return;
    }

    if (jobRecord.getChildKeys().size() > 0) {
      cancelChildren(jobRecord, null);
    }

    // No error handler present. So just mark this job as CANCELED.
    if (log.isLoggable(Level.FINEST)) {
      log.finest("Marking " + jobRecord + " as CANCELED");
    }
    jobRecord.setState(State.CANCELED);
    UpdateSpec updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    updateSpec.getNonTransactionalGroup().includeJob(jobRecord);
    if (jobRecord.isExceptionHandlerSpecified()) {
      executeExceptionHandler(updateSpec, jobRecord, new CancellationException(), true);
    }
    backEnd.save(updateSpec);
  }

  private void handleExceptionDuringRun(JobRecord jobRecord, JobRecord rootJobRecord,
      String currentRunGUID, Throwable caughtException) {
    long attemptNumber = jobRecord.getAttemptNumber();
    long maxAttempts = jobRecord.getMaxAttempts();
    if (jobRecord.isCallExceptionHandler()) {
      log.log(Level.INFO,
          "An exception occurred when attempting to execute exception hander job " + jobRecord
          + ". ", caughtException);
    } else {
      log.log(Level.INFO, "An exception occurred when attempting to run " + jobRecord + ". "
          + "This was attempt number " + attemptNumber + " of " + maxAttempts + ".",
          caughtException);
    }
    if (jobRecord.isIgnoreException()) {
      return;
    }
    UpdateSpec updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    ExceptionRecord exceptionRecord = new ExceptionRecord(
        jobRecord.getRootJobKey(), jobRecord.getKey(), currentRunGUID, caughtException);
    updateSpec.getNonTransactionalGroup().includeException(exceptionRecord);
    Key exceptionKey = exceptionRecord.getKey();
    jobRecord.setExceptionKey(exceptionKey);
    if (jobRecord.isCallExceptionHandler() || attemptNumber >= maxAttempts) {
      jobRecord.setState(State.STOPPED);
      updateSpec.getNonTransactionalGroup().includeJob(jobRecord);
      if (jobRecord.isExceptionHandlerSpecified()) {
        cancelChildren(jobRecord, null);
        executeExceptionHandler(updateSpec, jobRecord, caughtException, false);
      } else {
        if (null != jobRecord.getExceptionHandlingAncestorKey()) {
          cancelChildren(jobRecord, null);
          // current job doesn't have an error handler. So just delegate it to the
          // nearest ancestor that has one.
          PipelineTask handleChildExceptionPipelineTask = new HandleChildExceptionTask(
              jobRecord.getExceptionHandlingAncestorKey(), jobRecord.getKey(),
              jobRecord.getQueueSettings());
          updateSpec.getFinalTransaction().registerTask(handleChildExceptionPipelineTask);
        } else {
          rootJobRecord.setState(State.STOPPED);
          rootJobRecord.setExceptionKey(exceptionKey);
          updateSpec.getNonTransactionalGroup().includeJob(rootJobRecord);
        }
      }
      backEnd.save(updateSpec);
    } else {
      jobRecord.setState(State.RETRY);
      long backoffFactor = jobRecord.getBackoffFactor();
      long backoffSeconds = jobRecord.getBackoffSeconds();
      RunJobTask task =
          new RunJobTask(jobRecord.getKey(), attemptNumber, jobRecord.getQueueSettings());
      task.getQueueSettings().setDelayInSeconds(
          backoffSeconds * (long) Math.pow(backoffFactor, attemptNumber));
      updateSpec.getFinalTransaction().includeJob(jobRecord);
      updateSpec.getFinalTransaction().registerTask(task);
      backEnd.saveWithJobStateCheck(updateSpec, jobRecord.getKey(),
          State.WAITING_TO_RUN, State.RETRY);
    }
  }

  /**
   * @param updateSpec UpdateSpec to use
   * @param jobRecord record of the job with exception handler to execute
   * @param caughtException failure cause
   * @param ignoreException if failure should be ignored (used for cancellation)
   */
  private void executeExceptionHandler(UpdateSpec updateSpec, JobRecord jobRecord,
      Throwable caughtException, boolean ignoreException) {
    updateSpec.getNonTransactionalGroup().includeJob(jobRecord);
    String errorHandlingGraphGuid = GUIDGenerator.nextGUID();
    Job<?> jobInstance = jobRecord.getJobInstanceInflated().getJobInstanceDeserialized();

    JobSetting[] settings =  new JobSetting[0];
    JobRecord errorHandlingJobRecord =
        new JobRecord(jobRecord, errorHandlingGraphGuid, jobInstance, true, settings, backEnd.getSerializationStrategy());
    pinServiceAndVersion(errorHandlingJobRecord, settings);
    errorHandlingJobRecord.setOutputSlotInflated(jobRecord.getOutputSlotInflated());
    errorHandlingJobRecord.setIgnoreException(ignoreException);
    registerNewJobRecord(updateSpec, errorHandlingJobRecord,
        new Object[] {new ImmediateValue<>(caughtException)});
  }

  private void cancelChildren(JobRecord jobRecord, Key failedChildKey) {
    for (Key childKey : jobRecord.getChildKeys()) {
      if (!childKey.equals(failedChildKey)) {
        CancelJobTask cancelJobTask = new CancelJobTask(childKey, jobRecord.getQueueSettings());
        backEnd.enqueue(cancelJobTask);
      }
    }
  }

  private void handleChildException(HandleChildExceptionTask handleChildExceptionTask) {
    Key jobKey = handleChildExceptionTask.getKey();
    Key failedChildKey = handleChildExceptionTask.getFailedChildKey();
    JobRecord jobRecord = queryJobOrAbandonTask(jobKey, InflationType.FOR_RUN);
    jobRecord.getQueueSettings().merge(handleChildExceptionTask.getQueueSettings());
    Key rootJobKey = jobRecord.getRootJobKey();
    log.info("Running pipeline job " + jobKey.getName() + " exception handler; UI at "
        + PipelineServlet.makeViewerUrl(jobRecord.getPipelineRunId(), jobRecord.getJobRunId()));
    JobRecord rootJobRecord;
    if (rootJobKey.equals(jobKey)) {
      rootJobRecord = jobRecord;
    } else {
      rootJobRecord = queryJobOrAbandonTask(rootJobKey, InflationType.NONE);
    }
    if (rootJobRecord.getState() == State.STOPPED) {
      log.warning("The pipeline has been stopped: " + rootJobRecord);
      throw new AbandonTaskException();
    }
    // TODO(user): add jobState check
    JobRecord failedJobRecord =
        queryJobOrAbandonTask(failedChildKey, InflationType.FOR_OUTPUT);
    UpdateSpec updateSpec = new UpdateSpec(rootJobKey);
    cancelChildren(jobRecord, failedChildKey);
    executeExceptionHandler(updateSpec, jobRecord, failedJobRecord.getException(), false);
    backEnd.save(updateSpec);
  }

  /**
   * Finalize the job with the given key.
   * <p>
   * We fetch the {@link JobRecord} from the data store and then fetch its
   * finalize {@link Barrier} and all of the {@link Slot Slots} in the finalize
   * {@code Barrier} (which should be filled.) We set the finalize Barrier to
   * released and save it. We fetch the job's output Slot. We use the values of
   * the filled finalize {@code Slots} to populate the output slot. We set the
   * state of the Job to {@code FINALIZED} and save the {@link JobRecord} and
   * the output slot. Finally we enqueue a {@link HandleSlotFilledTask} for the
   * output slot.
   *
   * @see "http://goto/java-pipeline-model"
   */
  private void finalizeJob(FinalizeJobTask finalizeJobTask) {
    Key jobKey = finalizeJobTask.getJobKey();
    // Get the JobRecord, its finalize Barrier, all the slots in the
    // finalize Barrier, and the job's output Slot.
    JobRecord jobRecord = queryJobOrAbandonTask(jobKey, InflationType.FOR_FINALIZE);
    jobRecord.getQueueSettings().merge(finalizeJobTask.getQueueSettings());
    switch (jobRecord.getState()) {
      case WAITING_TO_FINALIZE:
        // OK, proceed
        break;
      case WAITING_TO_RUN:
      case RETRY:
        throw new RuntimeException("" + jobRecord + " is in RETRY state");
      case STOPPED:
        log.info("This job has been stoped " + jobRecord);
        return;
      case CANCELED:
        log.info("This job has already been canceled " + jobRecord);
        return;
      case FINALIZED:
        log.info("This job has already been run " + jobRecord);
        return;
    }
    Barrier finalizeBarrier = jobRecord.getFinalizeBarrierInflated();
    if (null == finalizeBarrier) {
      throw new RuntimeException("" + jobRecord + " has not been inflated");
    }
    Slot outputSlot = jobRecord.getOutputSlotInflated();
    if (null == outputSlot) {
      throw new RuntimeException("" + jobRecord + " has not been inflated.");
    }

    // release the finalize barrier now so that any concurrent
    // HandleSlotFilled tasks will stop trying
    finalizeBarrier.setReleased();
    UpdateSpec updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    updateSpec.getOrCreateTransaction("releaseFinalizeBarrier").includeBarrier(finalizeBarrier);
    backEnd.save(updateSpec);

    updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    // Copy the finalize value to the output slot
    List<Object> finalizeArguments = finalizeBarrier.buildArgumentList();
    int numFinalizeArguments = finalizeArguments.size();
    Object finalizeValue;
    if (1 != numFinalizeArguments) {
      // let's assume the first argument is valid, log and move on
      log.severe(String.format("Expected 1 argument but got %d. key: %s", numFinalizeArguments, finalizeBarrier.getJobKey().toString()));
      log.severe(String.format("Argument list: %s", finalizeArguments.stream()
        .map( o -> (o == null) ? "null" : "%s: %s".formatted(o.getClass(), o.toString()))
        .collect(Collectors.joining(","))));
      //throw new RuntimeException(
      //    "Internal logic error: numFinalizeArguments=" + numFinalizeArguments);
      // this should now happen, is this coming from multiple tasks being executed?
      // wait for the last known added slot
      finalizeValue = Iterables.getLast(finalizeArguments.stream().filter(Objects::nonNull).toList(), null);
    } else {
      // normal scenario
      finalizeValue = finalizeArguments.get(0);
    }
    if (finalizeValue == null ) {
      log.warning("No finalize value found for key: " + jobRecord.getRootJobKey() + ", key: " + finalizeBarrier.getJobKey().toString() + " - is this legit?");
    }
    log.finest("Finalizing " + jobRecord + " with value=" + finalizeValue);
    outputSlot.fill(finalizeValue);

    // Change state of the job to FINALIZED and set the end time
    jobRecord.setState(State.FINALIZED);
    jobRecord.setEndTime(Instant.now());

    // Propagate the filler of the finalize slot to also be the filler of the
    // output slot. If there is no unique filler of the finalize slot then we
    // resort to assigning the current job as the filler job.
    Key fillerJobKey = getFinalizeSlotFiller(finalizeBarrier);
    if (null == fillerJobKey) {
      fillerJobKey = jobKey;
    }
    outputSlot.setSourceJobKey(fillerJobKey);

    // Save the job and the output slot
    updateSpec.getNonTransactionalGroup().includeJob(jobRecord);
    updateSpec.getNonTransactionalGroup().includeSlot(outputSlot);
    backEnd.save(updateSpec);

    // enqueue a HandleSlotFilled task
    HandleSlotFilledTask task =
        new HandleSlotFilledTask(outputSlot.getKey(), jobRecord.getQueueSettings());
    backEnd.enqueue(task);
  }

  /**
   * Return the unique value of {@link Slot#getSourceJobKey()} for all slots in
   * the finalize Barrier, if there is a unique such value. Otherwise return
   * {@code null}.
   *
   * @param finalizeBarrier A finalize Barrier
   * @return The unique slot filler for the finalize slots, or {@code null}
   */
  private Key getFinalizeSlotFiller(Barrier finalizeBarrier) {
    Key fillerJobKey = null;
    for (SlotDescriptor slotDescriptor : finalizeBarrier.getWaitingOnInflated()) {
      Key key = slotDescriptor.slot.getSourceJobKey();
      if (null != key) {
        if (null == fillerJobKey) {
          fillerJobKey = key;
        } else {
          if (!fillerJobKey.toString().equals(key.toString())) {
            // Found 2 non-equal values, return null
            return null;
          }
        }
      }
    }
    return fillerJobKey;
  }


  /**
   * Handle the fact that the slot with the given key has been filled.
   * <p>
   * For each barrier that is waiting on the slot, if all of the slots that the
   * barrier is waiting on are now filled then the barrier should be released.
   * Release the barrier by enqueueing an appropriate task (either
   * {@link RunJobTask} or {@link FinalizeJobTask}.
   */
  private void handleSlotFilled(HandleSlotFilledTask hsfTask) {
    Key slotKey = hsfTask.getSlotKey();
    Slot slot = querySlotOrAbandonTask(slotKey, true);
    List<Barrier> waitingList = slot.getWaitingOnMeInflated();
    if (null == waitingList) {
      throw new RuntimeException("Internal logic error: " + slot + " is not inflated");
    }
    // For each barrier that is waiting on the slot ...
    for (Barrier barrier : waitingList) {
      log.finest("Checking " + barrier);
      // unless the barrier has already been released,
      if (!barrier.isReleased()) {
        // we check whether the barrier should be released.
        boolean shouldBeReleased = true;
        if (null == barrier.getWaitingOnInflated()) {
          throw new RuntimeException("Internal logic error: " + barrier + " is not inflated.");
        }
        // For each slot that the barrier is waiting on...
        for (SlotDescriptor sd : barrier.getWaitingOnInflated()) {
          // see if it is full.
          if (!sd.slot.isFilled()) {
            log.finest("Not filled: " + sd.slot);
            shouldBeReleased = false;
            break;
          }
        }
        if (shouldBeReleased) {
          Key jobKey = barrier.getJobKey();
          JobRecord jobRecord = queryJobOrAbandonTask(jobKey, InflationType.NONE);
          jobRecord.getQueueSettings().merge(hsfTask.getQueueSettings());
          PipelineTask pipelineTask;
          switch (barrier.getType()) {
            case RUN:
              pipelineTask = new RunJobTask(jobKey, jobRecord.getQueueSettings());
              break;
            case FINALIZE:
              pipelineTask = new FinalizeJobTask(jobKey, jobRecord.getQueueSettings());
              break;
            default:
              throw new RuntimeException("Unknown barrier type " + barrier.getType());
          }
          backEnd.enqueue(pipelineTask);
        }
      }
    }
  }

  /**
   * Fills the slot with null value and calls handleSlotFilled
   */
  private void handleDelayedSlotFill(DelayedSlotFillTask task) {
    Key slotKey = task.getSlotKey();
    Slot slot = querySlotOrAbandonTask(slotKey, true);
    Key rootJobKey = task.getRootJobKey();
    UpdateSpec updateSpec = new UpdateSpec(rootJobKey);
    slot.fill(null);
    updateSpec.getNonTransactionalGroup().includeSlot(slot);
    backEnd.save(updateSpec);
    // re-reading Slot (in handleSlotFilled) is needed (to capture slot fill after this one)
    handleSlotFilled(new HandleSlotFilledTask(slotKey, task.getQueueSettings()));
  }

  /**
   * Queries for the job with the given key from the data store and if the job
   * is not found then throws an {@link AbandonTaskException}.
   *
   * @param key The key of the JobRecord to be fetched
   * @param inflationType Specifies the manner in which the returned JobRecord
   *        should be inflated.
   * @return A {@code JobRecord}, possibly with a partially-inflated associated
   *         graph of objects.
   * @throws AbandonTaskException If Either the JobRecord or any of the
   *         associated Slots or Barriers are not found in the data store.
   */
  private JobRecord queryJobOrAbandonTask(Key key, InflationType inflationType) {
    try {
      return backEnd.queryJob(key, inflationType);
    } catch (NoSuchObjectException e) {
      log.log(
          Level.WARNING, "Cannot find some part of the job: " + key + ". Ignoring the task.", e);
      throw new AbandonTaskException();
    }
  }

  /**
   * Queries the Slot with the given Key from the data store and if the Slot is
   * not found then throws an {@link AbandonTaskException}.
   *
   * @param key The Key of the slot to fetch.
   * @param inflate If this is {@code true} then the Barriers that are waiting
   *        on the Slot and the other Slots that those Barriers are waiting on
   *        will also be fetched from the data store and used to partially
   *        populate the graph of objects attached to the returned Slot. In
   *        particular: {@link Slot#getWaitingOnMeInflated()} will not return
   *        {@code null} and also that for each of the {@link Barrier Barriers}
   *        returned from that method {@link Barrier#getWaitingOnInflated()}
   *        will not return {@code null}.
   * @return A {@code Slot}, possibly with a partially-inflated associated graph
   *         of objects.
   * @throws AbandonTaskException If either the Slot or the associated Barriers
   *         and slots are not found in the data store.
   */
  private Slot querySlotOrAbandonTask(Key key, boolean inflate) {
    try {
      return backEnd.querySlot(key, inflate);
    } catch (NoSuchObjectException e) {
      log.log(Level.WARNING, "Cannot find the slot: " + key + ". Ignoring the task.", e);
      throw new AbandonTaskException();
    }
  }

  /**
   * @param slot delayed value slot
   */
  public static void registerDelayedValue(
      UpdateSpec spec, JobRecord generatorJobRecord, long delaySec, Slot slot) {
    Key rootKey = generatorJobRecord.getRootJobKey();
    QueueSettings queueSettings = generatorJobRecord.getQueueSettings();
    DelayedSlotFillTask task = new DelayedSlotFillTask(slot, delaySec, rootKey , queueSettings);
    spec.getFinalTransaction().registerTask(task);
  }
}
