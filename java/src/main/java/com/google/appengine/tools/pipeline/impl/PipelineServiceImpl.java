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

package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedInject;
import lombok.AllArgsConstructor;

import javax.inject.Inject;

/**
 * Implements {@link PipelineService} by delegating to {@link PipelineManager}.
 *
 * q: why doesn't PipelineManager just directly implement PipelineService?
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class PipelineServiceImpl implements PipelineService {

  PipelineManager pipelineManager;
  PipelineBackEnd backend;

  @Override
  public PipelineBackEnd.Options getBackendOptions() {
    return pipelineManager.getBackendOptions();
  }


  @Override
  public String startNewPipeline(Job0<?> jobInstance, JobSetting... settings) {
    return pipelineManager.startNewPipeline(settings, jobInstance);
  }

  @Override
  public <T1> String startNewPipeline(Job1<?, T1> jobInstance, T1 arg1, JobSetting... settings) {
    return pipelineManager.startNewPipeline(settings, jobInstance, arg1);
  }

  @Override
  public <T1, T2> String startNewPipeline(Job2<?, T1, T2> jobInstance, T1 arg1, T2 arg2,
      JobSetting... settings) {
    return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2);
  }

  @Override
  public <T1, T2, T3> String startNewPipeline(Job3<?, T1, T2, T3> jobInstance, T1 arg1, T2 arg2,
      T3 arg3, JobSetting... settings) {
    return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3);
  }

  @Override
  public <T1, T2, T3, T4> String startNewPipeline(Job4<?, T1, T2, T3, T4> jobInstance, T1 arg1,
      T2 arg2, T3 arg3, T4 arg4, JobSetting... settings) {
    return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4);
  }

  @Override
  public <T1, T2, T3, T4, T5> String startNewPipeline(Job5<?, T1, T2, T3, T4, T5> jobInstance,
      T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, JobSetting... settings) {
    return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4, arg5);
  }

  @Override
  public <T1, T2, T3, T4, T5, T6> String startNewPipeline(
      Job6<?, T1, T2, T3, T4, T5, T6> jobInstance, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5,
      T6 arg6, JobSetting... settings) {
    return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4, arg5,
        arg6);
  }

  @Override
  public String startNewPipelineUnchecked(Job<?> jobInstance, Object[] arguments,
      JobSetting... settings) {
    return pipelineManager.startNewPipeline(settings, jobInstance, arguments);
  }

  @Override
  public void stopPipeline(String jobHandle) throws NoSuchObjectException {
    pipelineManager.stopJob(jobHandle);
  }

  @Override
  public void cancelPipeline(String jobHandle) throws NoSuchObjectException {
    pipelineManager.cancelJob(jobHandle);
  }

  @Override
  public void deletePipelineRecords(String pipelineHandle) throws NoSuchObjectException,
      IllegalStateException {
    deletePipelineRecords(pipelineHandle, false);
  }

  @Override
  public void deletePipelineRecords(String pipelineHandle, boolean force)
      throws NoSuchObjectException, IllegalStateException {
    pipelineManager.deletePipelineRecords(pipelineHandle, force);
  }

  @Override
  public JobInfo getJobInfo(String jobHandle) throws NoSuchObjectException {
    return pipelineManager.getJob(jobHandle);
  }

  @Override
  public void submitPromisedValue(String promiseHandle, Object value)
      throws NoSuchObjectException, OrphanedObjectException {
    pipelineManager.submitPromisedValue(promiseHandle, value);
  }
}
