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

package com.google.appengine.tools.pipeline.impl.tasks;

import com.google.cloud.datastore.Key;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import lombok.ToString;

import java.util.Properties;

/**
 * A subclass of {@link ObjRefTask} used to request that the job
 * with the specified key should be run.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
@ToString
public class RunJobTask extends ObjRefTask {

  private final Long attemptNumber;

  public RunJobTask(Key jobKey, Long attemptNumber, QueueSettings queueSettings) {
    super(Type.RUN_JOB, "runJob", jobKey, queueSettings);
    this.attemptNumber = attemptNumber;
  }

  public RunJobTask(Key jobKey, QueueSettings queueSettings) {
    this(jobKey, null, queueSettings);
  }

  // NOTE: this IS used via reflection ... important
  protected RunJobTask(Type type, String taskName, Properties properties) {
    super(type, taskName, properties);
    attemptNumber = null;
  }

  @Override
  public String getTaskName() {
    String name = super.getTaskName();
    if (null != attemptNumber) {
      name = name + "-attemptNumber-" + attemptNumber;
    }
    return name;
  }

  public Key getJobKey() {
    return getKey();
  }
}
