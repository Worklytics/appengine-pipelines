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

package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.SlotId;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.model.SlotDescriptor;
import com.google.appengine.tools.pipeline.impl.util.JsonUtils;
import com.google.appengine.tools.pipeline.util.Pair;
import com.google.cloud.datastore.Key;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for generating the Json that is consumed by the
 * JavaScript file status.js.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
class JsonGenerator {

  private static final String PIPELINE_ID = "pipelineId";
  private static final String ROOT_PIPELINE_ID = "rootPipelineId";
  private static final String SLOTS = "slots";
  private static final String PIPELINES = "pipelines";
  private static final String CURSOR = "cursor";
  private static final String SLOT_STATUS = "status";
  private static final String FILLED_STATUS = "filled";
  private static final String WAITING_STATUS = "waiting";
  private static final String SLOT_VALUE = "value";
  private static final String SLOT_FILL_TIME = "fillTimeMs";
  private static final String SLOT_SOURCE_JOB = "fillerPipelineId";
  private static final String JOB_CLASS = "classPath";
  private static final String JOB_STATUS = "status";
  private static final String RUN_STATUS = "run";
  private static final String DONE_STATUS = "done";
  private static final String ABORTED_STATUS = "aborted";
  private static final String RETRY_STATUS = "retry";
  private static final String CANCELED_STATUS = "canceled";

  // private static final String FINALIZING_STATUS = "finalizing";
  private static final String JOB_START_TIME = "startTimeMs";
  private static final String JOB_END_TIME = "endTimeMs";
  private static final String JOB_CHILDREN = "children";
  // List of positional argument slot dictionaries.
  private static final String JOB_ARGS = "args";
  // Dictionary of output slot dictionaries.
  private static final String JOB_OUTPUTS = "outputs";
  // Queue on which this pipeline is running.
  private static final String JOB_QUEUE_NAME = "queueName";
  // List of Slot Ids after which this pipeline runs.
  private static final String JOB_AFTER_SLOT_KEYS = "afterSlotKeys";
  // Number of the current attempt, starting at 1.
  private static final String JOB_CURRENT_ATTEMPT = "currentAttempt";
  // Maximum number of attempts before aborting
  private static final String JOB_MAX_ATTEMPTS = "maxAttempts";
  // Constant factor for backoff before retrying
  private static final String JOB_BACKOFF_SECONDS = "backoffSeconds";
  // Exponential factor for backoff before retrying
  private static final String JOB_BACKOFF_FACTOR = "backoffFactor";
  // Dictionary of keyword argument slot dictionaries.
  private static final String JOB_KWARGS = "kwargs";
  // Why the pipeline failed during the last retry, if there was a failure; may be empty
  private static final String JOB_LAST_RETRY_MESSAGE = "lastRetryMessage";
  // For root pipelines, why the pipeline was aborted if it was aborted; may be empty
  private static final String JOB_ABORT_MESSAGE = "abortMessage"; // For root
  private static final String DEFAULT_OUTPUT_NAME = "default";
  private static final String JOB_STATUS_CONSOLE_URL = "statusConsoleUrl";

  public static String pipelineRootsToJson(
      Pair<? extends Iterable<JobRecord>, String> pipelineRoots) {
    Map<String, Object> mapRepresentation = rootsToMapRepresentation(pipelineRoots);
    return JsonUtils.mapToJson(mapRepresentation);
  }

  public static String pipelineObjectsToJson(PipelineObjects pipelineObjects) {
    Map<String, Object> mapRepresentation = objectsToMapRepresentation(pipelineObjects);
    return JsonUtils.mapToJson(mapRepresentation);
  }

  @VisibleForTesting
  static Map<String, Object> objectsToMapRepresentation(PipelineObjects pipelineObjects) {
    Map<String, Map<String, Object>> slotMap = new HashMap<>(pipelineObjects.getSlots().size());
    Map<String, Map<String, Object>> jobMap = new HashMap<>(pipelineObjects.getJobs().size());
    Map<String, Object> topLevel = new HashMap<>(4);
    topLevel.put(ROOT_PIPELINE_ID, JobRunId.of(pipelineObjects.getRootJob().getKey()).asEncodedString());
    topLevel.put(SLOTS, slotMap);
    topLevel.put(PIPELINES, jobMap);

    //somehow, there are conditions in which this is building cyclic object graphs, which newer versions of JSONObject
    // attempt to serialize and end up with stack overflows
    for (Slot slot : pipelineObjects.getSlots().values()) {
      slotMap.put(SlotId.of(slot.getKey()).asEncodedString(), buildMapRepresentation(slot));
    }
    for (JobRecord jobRecord : pipelineObjects.getJobs().values()) {
      jobMap.put(JobRunId.of(jobRecord.getKey()).asEncodedString(), buildMapRepresentation(jobRecord));
    }
    return topLevel;
  }

  private static Map<String, Object> rootsToMapRepresentation(
      Pair<? extends Iterable<JobRecord>, String> pipelineRoots) {
    List<Map<String, Object>> jobList = new LinkedList<>();
    Map<String, Object> topLevel = new HashMap<>(3);
    for (JobRecord rootRecord : pipelineRoots.getFirst()) {
      Map<String, Object> mapRepresentation = buildMapRepresentation(rootRecord);
      mapRepresentation.put(PIPELINE_ID, JobRunId.of(rootRecord.getKey()).asEncodedString());
      jobList.add(mapRepresentation);
    }
    topLevel.put(PIPELINES, jobList);
    if (pipelineRoots.getSecond() != null) {
      topLevel.put(CURSOR, pipelineRoots.getSecond());
    }
    return topLevel;
  }

  private static Map<String, Object> buildMapRepresentation(Slot slot) {
    Map<String, Object> map = new HashMap<>(5);
    String statusString = (slot.isFilled() ? FILLED_STATUS : WAITING_STATUS);
    map.put(SLOT_STATUS, statusString);
    try {
      map.put(SLOT_VALUE, slot.getValue());
    } catch (RuntimeException ex) {
      map.put(SLOT_VALUE, ex);
    }
    Instant fillTime = slot.getFillTime();
    if (null != fillTime) {
      map.put(SLOT_FILL_TIME, fillTime.toEpochMilli());
    }
    Key sourceJobKey = slot.getSourceJobKey();
    if (null != sourceJobKey) {
      map.put(SLOT_SOURCE_JOB, JobRunId.of(sourceJobKey).asEncodedString());
    }
    return map;
  }

  private static Map<String, Object> buildMapRepresentation(JobRecord jobRecord) {
    Map<String, Object> map = new HashMap<>(5);
    String jobClass = jobRecord.getRootJobDisplayName();
    if (jobClass == null) {
      JobInstanceRecord jobInstanceInflated = jobRecord.getJobInstanceInflated();
      if (null != jobInstanceInflated) {
        jobClass = jobInstanceInflated.getJobDisplayName();
      } else {
        jobClass = "";
      }
    }
    map.put(JOB_CLASS, jobClass);
    String statusString = null;
    switch (jobRecord.getState()) {
      case WAITING_TO_RUN:
        statusString = WAITING_STATUS;
        break;
      case WAITING_TO_FINALIZE:
        statusString = RUN_STATUS;
        break;
      case FINALIZED:
        statusString = DONE_STATUS;
        break;
      case STOPPED:
        statusString = ABORTED_STATUS;
        break;
      case RETRY:
        statusString = RETRY_STATUS;
        break;
      case CANCELED:
        statusString = CANCELED_STATUS;
        break;
      default:
        break;
    }
    map.put(JOB_STATUS, statusString);
    Instant startTime = jobRecord.getStartTime();
    if (null != startTime) {
      map.put(JOB_START_TIME, startTime.toEpochMilli());
    }
    Instant endTime = jobRecord.getEndTime();
    if (null != endTime) {
      map.put(JOB_END_TIME, endTime.toEpochMilli());
    }
    map.put(JOB_CHILDREN, buildArrayRepresentation(jobRecord.getChildKeys()));
    List<Map<String, Object>> argumentListRepresentation = new LinkedList<>();
    List<String> waitingOnRepresentation = new LinkedList<>();
    Barrier runBarrierInflated = jobRecord.getRunBarrierInflated();
    if (runBarrierInflated != null) {
      populateJobArgumentRepresentation(argumentListRepresentation, waitingOnRepresentation,
          runBarrierInflated.getWaitingOnInflated());
    }
    map.put(JOB_ARGS, argumentListRepresentation);
    map.put(JOB_AFTER_SLOT_KEYS, waitingOnRepresentation);
    Map<String, String> allOutputs = new HashMap<>();
    SlotId outputSlotId = SlotId.of(jobRecord.getOutputSlotKey());
    // Python Pipeline has the notion of multiple outputs with a distinguished
    // output named "default". We don't have that notion. We have only
    // one output and so we put it in "default".
    allOutputs.put(DEFAULT_OUTPUT_NAME, outputSlotId.asEncodedString());
    map.put(JOB_OUTPUTS, allOutputs);
    map.put(JOB_QUEUE_NAME,
        Optional.fromNullable(jobRecord.getQueueSettings().getOnQueue()).or(""));
    map.put(JOB_CURRENT_ATTEMPT, jobRecord.getAttemptNumber());
    map.put(JOB_MAX_ATTEMPTS, jobRecord.getMaxAttempts());
    map.put(JOB_BACKOFF_SECONDS, jobRecord.getBackoffSeconds());
    map.put(JOB_BACKOFF_FACTOR, jobRecord.getBackoffFactor());
    map.put(JOB_KWARGS, new HashMap<String, String>());
    Throwable error = jobRecord.getException();
    if (error != null) {
      switch (jobRecord.getState()) {
        case STOPPED:
        case CANCELED:
          map.put(JOB_ABORT_MESSAGE, error.getMessage());
          break;
        default:
          map.put(JOB_LAST_RETRY_MESSAGE, error.getMessage());
      }
    }
    if (jobRecord.getStatusConsoleUrl() != null) {
      map.put(JOB_STATUS_CONSOLE_URL, jobRecord.getStatusConsoleUrl());
    }
    return map;
  }

  private static void populateJobArgumentRepresentation(
      List<Map<String, Object>> argumentListRepresentation, List<String> waitingOnRepresentation,
      List<SlotDescriptor> slotDescriptors) {
    for (SlotDescriptor slotDescriptor : slotDescriptors) {
      Slot slot = slotDescriptor.slot;
      SlotId slotId = SlotId.of(slot.getKey());
      if (slotDescriptor.isPhantom()) {
        waitingOnRepresentation.add(slotId.asEncodedString());
      } else {
        argumentListRepresentation.add(buildArgumentRepresentation(slotDescriptor.slot));
      }
    }
  }

  private static Map<String, Object> buildArgumentRepresentation(Slot slot) {
    Map<String, Object> map = new HashMap<>(3);
    if (slot.isFilled()) {
      map.put("type", "value");
      try {
        map.put("value", slot.getValue());
      } catch (RuntimeException ex) {
        map.put("value", ex);
      }
    } else {
      map.put("type", "slot");
      map.put("slot_key", SlotId.of(slot.getKey()).asEncodedString());
    }

    return map;
  }

  private static String[] buildArrayRepresentation(List<Key> listOfKeys) {
    String[] arrayOfIds = new String[listOfKeys.size()];
    int i = 0;
    for (Key key : listOfKeys) {
      arrayOfIds[i++] = JobRunId.of(key).asEncodedString();
    }
    return arrayOfIds;
  }
}
