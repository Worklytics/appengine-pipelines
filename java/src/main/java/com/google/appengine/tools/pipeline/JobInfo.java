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

package com.google.appengine.tools.pipeline;

/**
 * A record about a job that has been registered with the framework. A {@code
 * JobInfo} is obtained via the method
 * {@link PipelineService#getJobInfo(JobRunId)}.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface JobInfo {

  /**
   * The state of the job.
   */
  static enum State {
    /**
     * Job is currently executing.
     */
    RUNNING, 
    /**
     * Job has completed successfully.
     */
    COMPLETED_SUCCESSFULLY, 
    /**
     * Job was stopped through {@link PipelineService#stopPipeline(JobRunId)}.
     */
    STOPPED_BY_REQUEST, 
    /**
     * Job execution was stopped due to unhandled failure. 
     */
    STOPPED_BY_ERROR, 
    /**
     * Job has failed and is going to retry later.
     */
    WAITING_TO_RETRY,
    /**
     * Job was cancelled either through
     * {@link PipelineService#cancelPipeline(JobRunId)} or due to unhandled
     * failure in a sibling job.
     */
    CANCELED_BY_REQUEST
  }

  JobRunId getPipelineRunId();

  JobRunId getJobRunId();



  /**
   * Get the job's {@link State}.
   */
  State getJobState();

  /**
   * If the job's {@link State State} is {@link State#COMPLETED_SUCCESSFULLY
   * COMPLETED_SUCCESSFULLY}, returns the job's output. Otherwise returns
   * {@code null}.
   */
  Object getOutput();

  /**
   * If the job's {@link State State} is {@link State#STOPPED_BY_ERROR
   * STOPPED_BY_ERROR}, returns the error stack trace. Otherwise returns {@code
   * null}
   */
  String getError();
  
  /**
   * If the job's {@link State State} is {@link State#STOPPED_BY_ERROR
   * STOPPED_BY_ERROR}, returns the error. Otherwise returns {@code
   * null}
   */
  Throwable getException();

}
