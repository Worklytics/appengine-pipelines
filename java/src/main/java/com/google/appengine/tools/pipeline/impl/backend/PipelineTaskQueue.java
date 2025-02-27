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

package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.tasks.Task;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;

/**
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public interface PipelineTaskQueue {


  /**
   * reference to a task by queue and task name.
   *
   * NOTE: we'll need to add project notion, if ever care about cross-project pipelines or something
   */
  @AllArgsConstructor(staticName = "of")
  @Value
  class TaskReference implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * the queue the task was enqueued to
     */
    String queue;

    /**
     * the name of the task (may have been auto-generated)
     */
    String taskName;
  }

  TaskReference enqueue(Task task);

  Collection<TaskReference> enqueue(final Collection<Task> tasks);
}
