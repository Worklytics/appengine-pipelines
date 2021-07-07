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
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.appengine.tools.pipeline.impl.util.EntityUtils;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;

import java.io.IOException;

/**
 * Job's state persistence.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class JobInstanceRecord extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-jobInstanceRecord";
  private static final String JOB_KEY_PROPERTY = "jobKey";
  private static final String JOB_CLASS_NAME_PROPERTY = "jobClassName";
  public static final String JOB_DISPLAY_NAME_PROPERTY = "jobDisplayName";
  private static final String INSTANCE_VALUE_PROPERTY = "value";

  // persistent
  private final Key jobKey;
  private final String jobClassName;
  private final String jobDisplayName;
  private final Object value;

  // transient
  private Job<?> jobInstance;
  private SerializationStrategy serializationStrategy;

  public JobInstanceRecord(JobRecord job, Job<?> jobInstance, SerializationStrategy serializationStrategy) {
    super(job.getRootJobKey(), job.getGeneratorJobKey(), job.getGraphGuid());
    jobKey = job.getKey();
    jobClassName = jobInstance.getClass().getName();
    jobDisplayName = jobInstance.getJobDisplayName();
    try {
      value = serializationStrategy.serializeValue(this, jobInstance);
    } catch (IOException e) {
      throw new RuntimeException("Exception while attempting to serialize the jobInstance "
          + jobInstance, e);
    }
    this.serializationStrategy = serializationStrategy;
 }

  public JobInstanceRecord(Entity entity, SerializationStrategy serializationStrategy) {
    super(entity);
    jobKey = entity.getKey(JOB_KEY_PROPERTY);
    jobClassName = entity.getString(JOB_CLASS_NAME_PROPERTY);
    if (entity.contains(JOB_DISPLAY_NAME_PROPERTY)) {
      jobDisplayName = entity.getString(JOB_DISPLAY_NAME_PROPERTY);
    } else {
      jobDisplayName = jobClassName;
    }
    value = entity.getValue(INSTANCE_VALUE_PROPERTY).get();
    this.serializationStrategy = serializationStrategy;
  }

  @Override
  public Entity toEntity() {
    Entity.Builder entity = toProtoBuilder();
    entity.set(JOB_KEY_PROPERTY, jobKey);
    entity.set(JOB_CLASS_NAME_PROPERTY, jobClassName);
    EntityUtils.setLargeValue(entity, INSTANCE_VALUE_PROPERTY, value);
    entity.set(JOB_DISPLAY_NAME_PROPERTY, jobDisplayName);
    return entity.build();
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public Key getJobKey() {
    return jobKey;
  }

  /**
   * Returns the job class name for display purpose only.
   */
  public String getJobDisplayName() {
    return jobDisplayName;
  }

  public String getClassName() {
    return jobClassName;
  }

  public synchronized Job<?> getJobInstanceDeserialized() {
    if (null == jobInstance) {
      try {
        jobInstance = (Job<?>) serializationStrategy.deserializeValue(this, value);
      } catch (IOException e) {
        throw new RuntimeException(
            "Exception while attempting to deserialize jobInstance for " + jobKey, e);
      }
    }
    return jobInstance;
  }
}
