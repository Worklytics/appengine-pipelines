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

package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.IntValuedSetting;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;
import com.google.appengine.tools.pipeline.JobSetting.OnService;
import com.google.appengine.tools.pipeline.JobSetting.OnQueue;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.appengine.tools.pipeline.JobSetting.WaitForSetting;
import com.google.appengine.tools.pipeline.impl.FutureValueImpl;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineStandardGen2;
import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.appengine.tools.pipeline.impl.util.EntityUtils;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The Pipeline model object corresponding to a job.
 *
 * q: analogous to Spring Batch JobExecution??
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class JobRecord extends PipelineModelObject implements JobInfo {

  //TODO: very hacky, probably need to have a factory that builds these, and extend there
  @VisibleForTesting
  public static AppEngineEnvironment environment = new AppEngineStandardGen2();


  /**
   * The state of the job.
   *
   */
  public enum State {
    // TODO(user): document states (including valid transitions) and relation to JobInfo.State
    WAITING_TO_RUN,
    WAITING_TO_FINALIZE,
    FINALIZED,
    STOPPED,
    CANCELED,
    RETRY
  }

  public static final String EXCEPTION_HANDLER_METHOD_NAME = "handleException";

  /**
   * This enum serves as an input parameter to the method
   * {@link com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd#queryJob(
   * Key, InflationType)}. When fetching an
   * instance of {@code JobRecord} from the data store this enum specifies how
   * much auxiliary data should also be queried and used to inflate the instance
   * of {@code JobRecord}.
   *
   */
  public enum InflationType {
    /**
     * Do not inflate at all
     */
    NONE,

    /**
     * Inflate as necessary to run the job. In particular:
     * <ul>
     * <li>{@link JobRecord#getRunBarrierInflated()} will not return
     * {@code null}; and
     * <li>for the returned {@link Barrier}
     * {@link Barrier#getWaitingOnInflated()} will not return {@code null}; and
     * <li> {@link JobRecord#getOutputSlotInflated()} will not return
     * {@code null}; and
     * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return
     * {@code null}
     * </ul>
     */
    FOR_RUN,

    /**
     * Inflate as necessary to finalize the job. In particular:
     * <ul>
     * <li> {@link JobRecord#getOutputSlotInflated()} will not return
     * {@code null}; and
     * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return
     * {@code null}; and
     * <li>for the returned {@link Barrier} the method
     * {@link Barrier#getWaitingOnInflated()} will not return {@code null}.
     * </ul>
     */
    FOR_FINALIZE,

    /**
     * Inflate as necessary to retrieve the output of the job. In particular
     * {@link JobRecord#getOutputSlotInflated()} will not return {@code null}
     */
    FOR_OUTPUT;
  }

  public static final String DATA_STORE_KIND = "pipeline-job";
  // Data store entity property names
  private static final String JOB_INSTANCE_PROPERTY = "jobInstance";
  private static final String RUN_BARRIER_PROPERTY = "runBarrier";
  private static final String FINALIZE_BARRIER_PROPERTY = "finalizeBarrier";
  private static final String STATE_PROPERTY = "state";
  private static final String EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY =
      "exceptionHandlingAncestorKey";
  private static final String EXCEPTION_HANDLER_SPECIFIED_PROPERTY = "hasExceptionHandler";
  private static final String EXCEPTION_HANDLER_JOB_KEY_PROPERTY = "exceptionHandlerJobKey";
  private static final String EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY =
      "exceptionHandlerJobGraphGuid";
  private static final String CALL_EXCEPTION_HANDLER_PROPERTY = "callExceptionHandler";
  private static final String IGNORE_EXCEPTION_PROPERTY = "ignoreException";
  private static final String OUTPUT_SLOT_PROPERTY = "outputSlot";
  private static final String EXCEPTION_KEY_PROPERTY = "exceptionKey";
  private static final String START_TIME_PROPERTY = "startTime";
  private static final String END_TIME_PROPERTY = "endTime";
  private static final String CHILD_KEYS_PROPERTY = "childKeys";
  private static final String ATTEMPT_NUM_PROPERTY = "attemptNum";
  private static final String MAX_ATTEMPTS_PROPERTY = "maxAttempts";
  private static final String BACKOFF_SECONDS_PROPERTY = "backoffSeconds";
  private static final String BACKOFF_FACTOR_PROPERTY = "backoffFactor";
  private static final String ON_SERVICE_PROPERTY = "onService";
  private static final String ON_QUEUE_PROPERTY = "onQueue";
  private static final String ON_SERVICE_VERSION_PROPERTY = "onServiceVersion";
  private static final String CHILD_GRAPH_GUID_PROPERTY = "childGraphGuid";
  private static final String STATUS_CONSOLE_URL = "statusConsoleUrl";
  public static final String ROOT_JOB_DISPLAY_NAME = "rootJobDisplayName";

  public static final String IS_ROOT_JOB_PROPERTY = "isRootJob";

  /**
   * projectId for job; must be set
   */
  @Getter @NonNull
  private final String projectId;

  /**
   * namespace for Job, if any (otherwise default)
   */
  @Getter
  private final String namespace;

  @Getter
  private final Key jobInstanceKey;
  @Getter
  private final Key runBarrierKey;
  @Getter
  private final Key finalizeBarrierKey;
  @Getter
  private Key outputSlotKey;
  @Getter @Setter
  private State state;
  /**
   * Returns key of the nearest ancestor that has exceptionHandler method
   * overridden or <code>null</code> if none of them has it.
   */
  @Getter
  private Key exceptionHandlingAncestorKey;
  private boolean exceptionHandlerSpecified;
  @Getter
  private Key exceptionHandlerJobKey;
  @Getter @Setter
  private String exceptionHandlerJobGraphGuid;
  /**
   * If true then this job is exception handler and
   * {@code Job#handleException(Throwable)} should be called instead of <code>run
   * </code>.
   */
  @Getter
  private boolean callExceptionHandler;
  /**
   * If <code>true</code> then an exception during a job execution is ignored. It is
   * expected to be set to <code>true</code> for jobs that execute error handler due
   * to cancellation.
   */
  @Getter @Setter
  private boolean ignoreException;
  @Getter @Setter
  private Key exceptionKey;
  @Getter @Setter
  private Instant startTime;
  @Getter @Setter
  private Instant endTime;
  @Getter @Setter
  private String childGraphGuid;
  @Getter
  private List<Key> childKeys;
  @Getter
  private long attemptNumber;
  @Getter
  private long maxAttempts = JobSetting.MaxAttempts.DEFAULT;
  @Getter
  private long backoffSeconds = JobSetting.BackoffSeconds.DEFAULT;
  @Getter
  private long backoffFactor = JobSetting.BackoffFactor.DEFAULT;
  @Getter
  private final QueueSettings queueSettings = new QueueSettings();
  @Getter @Setter
  private String statusConsoleUrl;
  @Getter
  private String rootJobDisplayName;
  @Getter
  private Boolean isRootJob;


  // transient fields
  @Getter
  private Barrier runBarrierInflated;
  @Getter
  private Barrier finalizeBarrierInflated;
  @Getter
  private Slot outputSlotInflated;
  @Getter
  private JobInstanceRecord jobInstanceInflated;

  private Throwable exceptionInflated;

  /**
   * Re-constitutes an instance of this class from a Data Store entity.
   *
   * @param entity
   */
  public JobRecord(Entity entity) {
    super(entity);

    //TODO: new lib throws DatastoreException if any of these are undefined, rather than returning 'null
    // wrap with EntityUtils.getKey(entity, propertyName) ...?
    // something else?
    jobInstanceKey = entity.getKey(JOB_INSTANCE_PROPERTY);
    finalizeBarrierKey = entity.getKey(FINALIZE_BARRIER_PROPERTY);
    runBarrierKey = entity.getKey(RUN_BARRIER_PROPERTY);
    outputSlotKey = entity.getKey(OUTPUT_SLOT_PROPERTY);
    state = State.valueOf(entity.getString(STATE_PROPERTY));
    exceptionHandlingAncestorKey = EntityUtils.getKey(entity, EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY);
    exceptionHandlerSpecified = entity.contains(EXCEPTION_HANDLER_SPECIFIED_PROPERTY) ? entity.getBoolean(EXCEPTION_HANDLER_SPECIFIED_PROPERTY) : false;
    exceptionHandlerJobKey = EntityUtils.getKey(entity, EXCEPTION_HANDLER_JOB_KEY_PROPERTY);
    exceptionHandlerJobGraphGuid = EntityUtils.getString(entity, EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY);

    callExceptionHandler = entity.getBoolean(CALL_EXCEPTION_HANDLER_PROPERTY);
    ignoreException = entity.getBoolean(IGNORE_EXCEPTION_PROPERTY);
    childGraphGuid = EntityUtils.getString(entity, CHILD_GRAPH_GUID_PROPERTY);
    exceptionKey = EntityUtils.getKey(entity, EXCEPTION_KEY_PROPERTY);
    startTime = EntityUtils.getInstant(entity, START_TIME_PROPERTY);
    endTime = EntityUtils.getInstant(entity, END_TIME_PROPERTY);
    childKeys = (List<Key>) entity.getList(CHILD_KEYS_PROPERTY).stream().map(v -> v.get()).collect(Collectors.toList());
    attemptNumber = entity.getLong(ATTEMPT_NUM_PROPERTY);
    maxAttempts = entity.getLong(MAX_ATTEMPTS_PROPERTY);
    backoffSeconds = entity.getLong(BACKOFF_SECONDS_PROPERTY);
    backoffFactor = entity.getLong(BACKOFF_FACTOR_PROPERTY);
    queueSettings.setOnService(EntityUtils.getString(entity, ON_SERVICE_PROPERTY));
    queueSettings.setOnServiceVersion(EntityUtils.getString(entity, ON_SERVICE_VERSION_PROPERTY));
    queueSettings.setOnQueue(EntityUtils.getString(entity, ON_QUEUE_PROPERTY));

    statusConsoleUrl = EntityUtils.getString(entity, STATUS_CONSOLE_URL);
    rootJobDisplayName = EntityUtils.getString(entity, ROOT_JOB_DISPLAY_NAME);
    if (entity.contains(IS_ROOT_JOB_PROPERTY)) {
      isRootJob = entity.getBoolean(IS_ROOT_JOB_PROPERTY);
    }
    projectId = entity.getKey().getProjectId();
    namespace = entity.getKey().getNamespace();
  }


  /**
   * Constructs and returns a Data Store Entity that represents this model
   * object
   */
  @Override
  public Entity toEntity() {
    Entity.Builder builder = toProtoBuilder();
    builder.set(JOB_INSTANCE_PROPERTY, jobInstanceKey);
    builder.set(FINALIZE_BARRIER_PROPERTY, finalizeBarrierKey);
    builder.set(RUN_BARRIER_PROPERTY, runBarrierKey);
    builder.set(OUTPUT_SLOT_PROPERTY, outputSlotKey);
    builder.set(STATE_PROPERTY, state.toString());
    if (null != exceptionHandlingAncestorKey) {
      builder.set(EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY, exceptionHandlingAncestorKey);
    }
    if (exceptionHandlerSpecified) {
      builder.set(EXCEPTION_HANDLER_SPECIFIED_PROPERTY, Boolean.TRUE);
    }
    if (null != exceptionHandlerJobKey) {
      builder.set(EXCEPTION_HANDLER_JOB_KEY_PROPERTY, exceptionHandlerJobKey);
    }
    if (null != exceptionKey) {
      builder.set(EXCEPTION_KEY_PROPERTY, exceptionKey);
    }
    if (null != exceptionHandlerJobGraphGuid) {
      builder.set(EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY,
        StringValue.newBuilder(exceptionHandlerJobGraphGuid).setExcludeFromIndexes(true).build());
    }
    builder.set(CALL_EXCEPTION_HANDLER_PROPERTY, BooleanValue.newBuilder(callExceptionHandler).setExcludeFromIndexes(true).build());
    builder.set(IGNORE_EXCEPTION_PROPERTY, BooleanValue.newBuilder(ignoreException).setExcludeFromIndexes(true).build());
    if (childGraphGuid != null) {
      builder.set(CHILD_GRAPH_GUID_PROPERTY, StringValue.newBuilder(childGraphGuid).setExcludeFromIndexes(true).build());
    }
    if (startTime != null) {
      builder.set(START_TIME_PROPERTY, Timestamp.of(Date.from(startTime)));
    }
    if (endTime != null) {
      builder.set(END_TIME_PROPERTY, TimestampValue.newBuilder(Timestamp.of(Date.from(endTime))).setExcludeFromIndexes(true).build());
    }
    builder.set(CHILD_KEYS_PROPERTY, childKeys.stream().map(KeyValue::of).collect(Collectors.toList()));
    builder.set(ATTEMPT_NUM_PROPERTY, LongValue.newBuilder(attemptNumber).setExcludeFromIndexes(true).build());
    builder.set(MAX_ATTEMPTS_PROPERTY, LongValue.newBuilder(maxAttempts).setExcludeFromIndexes(true).build());
    builder.set(BACKOFF_SECONDS_PROPERTY, LongValue.newBuilder(backoffSeconds).setExcludeFromIndexes(true).build());
    builder.set(BACKOFF_FACTOR_PROPERTY, LongValue.newBuilder(backoffFactor).setExcludeFromIndexes(true).build());

    // good idea? or should we force jobs to have these values (take defaults, if nothing else?)
    if (queueSettings.getOnService() != null) {
      builder.set(ON_SERVICE_PROPERTY, StringValue.newBuilder(queueSettings.getOnService()).setExcludeFromIndexes(true).build());
    }
    if (queueSettings.getOnServiceVersion() != null) {
      builder.set(ON_SERVICE_VERSION_PROPERTY, StringValue.newBuilder(queueSettings.getOnServiceVersion()).setExcludeFromIndexes(true).build());
    }
    if (queueSettings.getOnQueue() != null) {
      builder.set(ON_QUEUE_PROPERTY, StringValue.newBuilder(queueSettings.getOnQueue()).setExcludeFromIndexes(true).build());
    }

    if (statusConsoleUrl != null) {
      builder.set(STATUS_CONSOLE_URL, StringValue.newBuilder(statusConsoleUrl).setExcludeFromIndexes(true).build());
    }
    if (rootJobDisplayName != null) {
      builder.set(IS_ROOT_JOB_PROPERTY, true);
      builder.set(ROOT_JOB_DISPLAY_NAME, rootJobDisplayName);
    }
    return builder.build();
  }

  /**
   * Constructs a new JobRecord given the provided data. The constructed
   * instance will be inflated in the sense that
   * {@link #getJobInstanceInflated()}, {@link #getFinalizeBarrierInflated()},
   * {@link #getOutputSlotInflated()} and {@link #getRunBarrierInflated()} will
   * all not return {@code null}. This constructor is used when a new JobRecord
   * is created during the run() method of a parent job. The parent job is also
   * known as the generator job.
   *
   * @param generatorJob The parent generator job of this job.
   * @param graphGUIDParam The GUID of the local graph of this job.
   * @param jobInstance The non-null user-supplied instance of {@code Job} that
   *        implements the Job that the newly created JobRecord represents.
   * @param callExceptionHandler The flag that indicates that this job should call
   *        {@code Job#handleException(Throwable)} instead of {@code run}.
   * @param settings Array of {@code JobSetting} to apply to the newly created
   *        JobRecord.
   */
  public JobRecord(JobRecord generatorJob,
                   String graphGUIDParam,
                   Job<?> jobInstance,
                   boolean callExceptionHandler,
                   JobSetting[] settings,
                   SerializationStrategy serializationStrategy
      ) {
    this(generatorJob.getRootJobKey(), null, generatorJob.getKey(), graphGUIDParam, jobInstance,
        callExceptionHandler, settings, generatorJob.getQueueSettings(), serializationStrategy);
    // If generator job has exception handler then it should be called in case
    // of this job throwing to create an exception handling child job.
    // If callExceptionHandler is true then this job is an exception handling
    // child and its exceptions should be handled by its parent's
    // exceptionHandlingAncestor to avoid infinite recursion.
    if (generatorJob.isExceptionHandlerSpecified() && !callExceptionHandler) {
      exceptionHandlingAncestorKey = generatorJob.getKey();
    } else {
      exceptionHandlingAncestorKey = generatorJob.getExceptionHandlingAncestorKey();
    }
    // Inherit settings from generator job
    Map<Class<? extends JobSetting>, JobSetting> settingsMap = new HashMap<>();
    for (JobSetting setting : settings) {
      settingsMap.put(setting.getClass(), setting);
    }
    if (!settingsMap.containsKey(StatusConsoleUrl.class)) {
      statusConsoleUrl = generatorJob.statusConsoleUrl;
    }
  }

  private JobRecord(Key rootJobKey, Key thisKey, Key generatorJobKey, String graphGUID,
      Job<?> jobInstance, boolean callExceptionHandler, JobSetting[] settings,
      QueueSettings parentQueueSettings, SerializationStrategy serializationStrategy) {
    super(rootJobKey, null, thisKey, generatorJobKey, graphGUID);
    jobInstanceInflated = new JobInstanceRecord(this, jobInstance, serializationStrategy);
    jobInstanceKey = jobInstanceInflated.getKey();
    exceptionHandlerSpecified = isExceptionHandlerSpecified(jobInstance);
    this.callExceptionHandler = callExceptionHandler;
    runBarrierInflated = new Barrier(Barrier.Type.RUN, this);
    runBarrierKey = runBarrierInflated.getKey();
    finalizeBarrierInflated = new Barrier(Barrier.Type.FINALIZE, this);
    finalizeBarrierKey = finalizeBarrierInflated.getKey();
    outputSlotInflated = new Slot(getRootJobKey(), getGeneratorJobKey(), getGraphGuid(), serializationStrategy);
    // Initially we set the filler of the output slot to be this Job.
    // During finalize we may reset it to the filler of the finalize slot.
    outputSlotInflated.setSourceJobKey(getKey());
    outputSlotKey = outputSlotInflated.getKey();
    childKeys = new LinkedList<>();
    state = State.WAITING_TO_RUN;
    for (JobSetting setting : settings) {
      applySetting(setting);
    }
    if (parentQueueSettings != null) {
      queueSettings.merge(parentQueueSettings);
    }
    String service = queueSettings.getOnService();

    if (service == null) {
      //no service set via jobSettings; so default to the currentModule / currentModuleVersion
      queueSettings.setOnService(environment.getService());
      queueSettings.setOnServiceVersion(environment.getVersion());
    } else if (queueSettings.getOnServiceVersion() == null) {
      //service set via JobSettings, but no specific version specified
      if (service.equals(environment.getService())) {
        queueSettings.setOnServiceVersion(environment.getVersion());
      } else {
        //TODO: omitting for now; don't know how to get the default version for a service generally ...
        //queueSettings.setOnServiceVersion(environment.getDefaultVersion(service));
      }
    }
    projectId = rootJobKey.getProjectId();
    namespace = JobSetting.getSettingValue(JobSetting.DatastoreNamespace.class, settings)
      .orElse(null);
  }

  // Constructor for Root Jobs (called by {@link #createRootJobRecord}).
  private JobRecord(Key key, Job<?> jobInstance, JobSetting[] settings, SerializationStrategy serializationStrategy) {
    // Root Jobs have their rootJobKey the same as their keys and provide null for generatorKey
    // and graphGUID. Also, callExceptionHandler is always false.
    this(key, key, null, null, jobInstance, false, settings, null, serializationStrategy);
    rootJobDisplayName = jobInstance.getJobDisplayName();
  }

  /**
   * A factory method for root jobs.
   *
   * @param projectId             The project id of the pipeline
   * @param jobInstance           The non-null user-supplied instance of {@code Job} that
   *                              implements the Job that the newly created JobRecord represents.
   * @param serializationStrategy
   * @param settings              Array of {@code JobSetting} to apply to the newly created
   *                              JobRecord.
   */
  public static JobRecord createRootJobRecord(String projectId,
                                              Job<?> jobInstance,
                                              SerializationStrategy serializationStrategy,
                                              JobSetting[] settings) {
    String namespace = JobSetting.getSettingValue(JobSetting.DatastoreNamespace.class, settings)
      .orElse(null);
    Key key = generateKey(projectId, namespace, DATA_STORE_KIND);
    return new JobRecord(key, jobInstance, settings, serializationStrategy);
  }


  public static boolean isExceptionHandlerSpecified(Job<?> jobInstance) {
    boolean result = false;
    Class<?> clazz = jobInstance.getClass();
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(EXCEPTION_HANDLER_METHOD_NAME)) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1 || !Throwable.class.isAssignableFrom(parameterTypes[0])) {
          throw new RuntimeException(method
              + " has invalid signature. It must have exactly one paramter of type "
              + "Throwable or any of its descendants");
        }
        result = true;
        // continue looping to check signature of all handleException methods
      }
    }
    return result;
  }

  private void applySetting(JobSetting setting) {
    if (setting instanceof WaitForSetting) {
      WaitForSetting wf = (WaitForSetting) setting;
      FutureValueImpl<?> fv = (FutureValueImpl<?>) wf.getValue();
      Slot slot = fv.getSlot();
      runBarrierInflated.addPhantomArgumentSlot(slot);
    } else if (setting instanceof IntValuedSetting) {
      int value = ((IntValuedSetting) setting).getValue();
      if (setting instanceof BackoffSeconds) {
        backoffSeconds = value;
      } else if (setting instanceof BackoffFactor) {
        backoffFactor = value;
      } else if (setting instanceof MaxAttempts) {
        maxAttempts = value;
      } else {
        throw new RuntimeException("Unrecognized JobSetting class " + setting.getClass().getName());
      }
    } else if (setting instanceof OnService) {
      queueSettings.setOnService(((JobSetting.OnService) setting).getValue());
    } else if (setting instanceof JobSetting.OnServiceVersion) {
      queueSettings.setOnServiceVersion(((JobSetting.OnServiceVersion) setting).getValue());
    } else if (setting instanceof OnQueue) {
      queueSettings.setOnQueue(((OnQueue) setting).getValue());
    } else if (setting instanceof StatusConsoleUrl) {
      statusConsoleUrl = ((StatusConsoleUrl) setting).getValue();
    } else if (setting instanceof JobSetting.DatastoreNamespace) {
      //ignore; applied in constructor, bc it's final
    } else {
      throw new RuntimeException("Unrecognized JobSetting class " + setting.getClass().getName());
    }
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  private static boolean checkForInflate(PipelineModelObject obj, Key expectedGuid, String name) {
    if (null == obj) {
      return false;
    }
    if (!expectedGuid.equals(obj.getKey())) {
      throw new IllegalArgumentException(
          "Wrong guid for " + name + ". Expected " + expectedGuid + " but was " + obj.getKey());
    }
    return true;
  }

  public void inflate(Barrier runBarrier, Barrier finalizeBarrier, Slot outputSlot,
      JobInstanceRecord jobInstanceRecord, ExceptionRecord exceptionRecord) {
    if (checkForInflate(runBarrier, runBarrierKey, "runBarrier")) {
      runBarrierInflated = runBarrier;
    }
    if (checkForInflate(finalizeBarrier, finalizeBarrierKey, "finalizeBarrier")) {
      finalizeBarrierInflated = finalizeBarrier;
    }
    if (checkForInflate(outputSlot, outputSlotKey, "outputSlot")) {
      outputSlotInflated = outputSlot;
    }
    if (checkForInflate(jobInstanceRecord, jobInstanceKey, "jobInstanceRecord")) {
      jobInstanceInflated = jobInstanceRecord;
    }
    if (checkForInflate(exceptionRecord, exceptionKey, "exception")) {
      exceptionInflated = exceptionRecord.getException();
    }
  }

  /**
   * Used to set exceptionHandling Job output to the same slot as the protected job.
   */
  public void setOutputSlotInflated(Slot outputSlot) {
    outputSlotInflated = outputSlot;
    outputSlotKey = outputSlot.getKey();
  }

  public boolean isExceptionHandlerSpecified() {
    // If this job is exception handler itself then it has exceptionHandlerSpecified
    // but it shouldn't delegate to it.
    return exceptionHandlerSpecified && (!isCallExceptionHandler());
  }

  public void incrementAttemptNumber() {
    attemptNumber++;
  }

  public void appendChildKey(Key key) {
    childKeys.add(key);
  }

  @Override
  public JobInfo.State getJobState() {
    switch (state) {
      case WAITING_TO_RUN:
      case WAITING_TO_FINALIZE:
        return JobInfo.State.RUNNING;
      case FINALIZED:
        return JobInfo.State.COMPLETED_SUCCESSFULLY;
      case CANCELED:
        return JobInfo.State.CANCELED_BY_REQUEST;
      case STOPPED:
        if (null == exceptionKey) {
          return JobInfo.State.STOPPED_BY_REQUEST;
        } else {
          return JobInfo.State.STOPPED_BY_ERROR;
        }
      case RETRY:
        return JobInfo.State.WAITING_TO_RETRY;
      default:
        throw new RuntimeException("Unrecognized state: " + state);
    }
  }

  @Override
  public Object getOutput() {
    if (null == outputSlotInflated) {
      return null;
    } else {
      return outputSlotInflated.getValue();
    }
  }

  @Override
  public String getError() {
    if (exceptionInflated == null) {
      return null;
    }
    return StringUtils.printStackTraceToString(exceptionInflated);
  }

  @Override
  public Throwable getException() {
    return this.exceptionInflated;
  }

  private String getJobInstanceString() {
    if (null == jobInstanceInflated) {
      return "jobInstanceKey=" + jobInstanceKey;
    }
    String jobClass = jobInstanceInflated.getClassName();
    return jobClass + (callExceptionHandler ? ".handleException" : ".run");
  }

  @Override
  public String toString() {
    return "JobRecord [" + getKeyName(getKey()) + ", " + state + ", " + getJobInstanceString()
        + ", callExceptionJHandler=" + callExceptionHandler + ", runBarrier="
        + runBarrierKey.getName() + ", finalizeBarrier=" + finalizeBarrierKey.getName()
        + ", outputSlot=" + outputSlotKey.getName() + ", rootJobDisplayName="
        + rootJobDisplayName + ", parent=" + getKeyName(getGeneratorJobKey()) + ", guid="
        + getGraphGuid() + ", childGuid=" + childGraphGuid + "]";
  }

  @VisibleForTesting
  public static Key key(String projectId, String namespace, String localJobHandle) {
    KeyFactory keyFactory = new KeyFactory(projectId, namespace);
    keyFactory.setKind(DATA_STORE_KIND);
    return keyFactory.newKey(localJobHandle);
  }

  @VisibleForTesting
  public static Key keyFromPipelineHandle(String pipelineHandle) {
    return Key.fromUrlSafe(pipelineHandle);
  }
}
