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

import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import lombok.*;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;
import java.util.SortedMap;

/**
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public interface PipelineTaskQueue {


  /**
   * represents specification of an async task invocation, prototypically via HTTP callback (webhook) via a 'queue'.
   *
   */
  @With
  @Builder
  @Value
  class TaskSpec  implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * supported execution methods for async
     */
    public enum Method {
      GET,
      POST,
      ;
    }

    /**
     * if non-null, a name used to refer to this task.  if null, a name will be auto-generated.
     * in former case, the name must be unique within the queue. if there is a task already existing with the same name (enqueued within last 7 days), the task will be rejected.
     */
    String name;

    //q: keep this? coupled to webhook execution case, and unclear to me why caller should care ...
    // why not leave to implementation to decide to send as GET or POST??
    @Builder.Default
    Method method = Method.GET;

    /**
     * the host to which to send the task
     */
    @NonNull
    String host;

    /**
     * callback for the task; relative to a host
     */
    @NonNull
    String callbackPath;

    /**
     * headers to pass to the task invocation
     */
    @Singular
    SortedMap<String, String> headers;

    /**
     * parameters to pass to the task
     */
    @Singular
    SortedMap<String, String> params;

    /**
     * a time at which to run the task, if any; (eg, should execute >= this time, subject to queue concurrency)
     */
    Instant scheduledExecutionTime;
  }

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

  TaskReference enqueue(PipelineTask pipelineTask);

  TaskReference enqueue(String queueName, TaskSpec build);

  Collection<TaskReference> enqueue(final Collection<PipelineTask> pipelineTasks);

  /**
   * deletes tasks from the queue, on best-efforts async basis.
   *
   * meant to be used only as an optimization, to avoid exec of tasks that are pointless (due to some other pipeline failure)
   *
   * @param taskReferences references to the tasks to delete
   */
  void deleteTasks(Collection<TaskReference> taskReferences);
}
