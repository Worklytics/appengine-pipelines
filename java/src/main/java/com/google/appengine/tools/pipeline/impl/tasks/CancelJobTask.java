// Copyright 2013 Google Inc.
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
 * A subclass of {@link ObjRefTask} used to request that the job with the specified key should be
 * canceled.
 *
 * @author maximf@google.com (Maxim Fateev)
 */
@ToString
public class CancelJobTask extends ObjRefTask {

  public CancelJobTask(Key jobKey, QueueSettings queueSettings) {
    super(Type.CANCEL_JOB, "cancelJob", jobKey, queueSettings);
  }

  protected CancelJobTask(Type type, String taskName, Properties properties) {
    super(type, taskName, properties);
  }

  public Key getJobKey() {
    return getKey();
  }
}
