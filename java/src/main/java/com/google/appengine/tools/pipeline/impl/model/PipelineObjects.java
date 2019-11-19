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

import com.google.appengine.api.datastore.Key;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.extern.java.Log;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A container for holding the results of querying for all objects associated
 * with a given root Job.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
@Log
public class PipelineObjects {

  @Getter
  private JobRecord rootJob;
  @Getter
  private Map<Key, JobRecord> jobs;
  @Getter
  private Map<Key, Slot> slots;
  @Getter
  private Map<Key, Barrier> barriers;
  @Getter
  private Map<Key, JobInstanceRecord> jobInstanceRecords;

  /**
   * The {@code PipelineObjects} takes ownership of the objects passed in. The
   * caller should not hold references to them.
   */
  public PipelineObjects(Key rootJobKey, Map<Key, JobRecord> jobs, Map<Key, Slot> slots,
      Map<Key, Barrier> barriers, Map<Key, JobInstanceRecord> jobInstanceRecords,
      Map<Key, ExceptionRecord> failureRecords) {
    this.jobInstanceRecords = jobInstanceRecords;
    this.barriers = barriers;
    this.jobs = jobs;
    this.slots = slots;

    Map<Key, String> jobToChildGuid = Maps.newHashMap();
    jobs.values().forEach(entry -> jobToChildGuid.put(entry.getKey(), entry.getChildGraphGuid()));

    this.rootJob = jobs.get(rootJobKey);
    if (null == rootJob) {
      throw new IllegalArgumentException("None of the jobs were the root job with key: " + rootJobKey);
    }

    for (Iterator<JobRecord> iter = jobs.values().iterator(); iter.hasNext(); ) {
      JobRecord job = iter.next();
      if (job != rootJob) {
        Key parentKey = job.getGeneratorJobKey();
        String graphGuid = job.getGraphGuid();
        if (parentKey == null || graphGuid == null) {
          log.info("Ignoring a non-root job with no parent or graphGuid -> " + job);
          iter.remove();
        } else if (!graphGuid.equals(jobToChildGuid.get(parentKey))) {
          log.info("Ignoring an orphaned job " + job + ", parent: " + jobs.get(parentKey));
          iter.remove();
        }
      }
    }

    //inflate slots
    for (Iterator<Slot> iter = slots.values().iterator(); iter.hasNext(); ) {
      Slot slot = iter.next();
      Key parentKey = slot.getGeneratorJobKey();
      String parentGuid = slot.getGraphGuid();
      if (parentKey == null && parentGuid == null
            || parentGuid != null && parentGuid.equals(jobToChildGuid.get(parentKey))) {
        slot.inflate(barriers);
      } else {
        log.info("Ignoring an orphaned slot " + slot + ", parent: " + jobs.get(parentKey));
        iter.remove();
      }
    }

    //inflate barriers
    for (Iterator<Barrier> iter = barriers.values().iterator(); iter.hasNext(); ) {
      Barrier barrier = iter.next();
      Key parentKey = barrier.getGeneratorJobKey();
      String parentGuid = barrier.getGraphGuid();
      if (parentKey == null && parentGuid == null
          || parentGuid != null && parentGuid.equals(jobToChildGuid.get(parentKey))) {
        barrier.inflate(slots);
      } else {
        log.info("Ignoring an orphaned Barrier " + barrier + ", parent: " + jobs.get(parentKey));
        iter.remove();
      }
    }

    //inflate job records
    for (JobRecord jobRec : jobs.values()) {
      Barrier runBarrier = barriers.get(jobRec.getRunBarrierKey());
      Barrier finalizeBarrier = barriers.get(jobRec.getFinalizeBarrierKey());
      Slot outputSlot = slots.get(jobRec.getOutputSlotKey());
      JobInstanceRecord jobInstanceRecord = jobInstanceRecords.get(jobRec.getJobInstanceKey());
      ExceptionRecord failureRecord = null;
      Key failureKey = jobRec.getExceptionKey();
      if (null != failureKey) {
        failureRecord = failureRecords.get(failureKey);
      }
      jobRec.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord, failureRecord);
    }
  }
}
