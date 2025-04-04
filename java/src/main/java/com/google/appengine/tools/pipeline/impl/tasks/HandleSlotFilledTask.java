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
 * A subclass of {@link ObjRefTask} used to request that the Pipeline framework
 * handle the fact that the specified slot has been filled.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
@ToString
public class HandleSlotFilledTask extends ObjRefTask {

  /**
   * This constructor is used on the sending side. That is, it is used to
   * construct a {@code HandleSlotFilledTask}to be enqueued.
   * <p>
   *
   * @param slotKey The key of the Slot whose filling is to be handled
   */
  public HandleSlotFilledTask(Key slotKey, QueueSettings queueSettings) {
    super(Type.HANDLE_SLOT_FILLED, "handleSlotFilled", slotKey, queueSettings);
  }

  protected HandleSlotFilledTask(Type type, String taskName, Properties properties) {
    super(type, taskName, properties);
  }

  public Key getSlotKey() {
    return getKey();
  }
}
