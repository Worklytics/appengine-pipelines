// Copyright 2014 Google Inc.
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

import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import lombok.AllArgsConstructor;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * @author ozarov@google.com (Arie Ozarov)
 */
@Singleton
@AllArgsConstructor(onConstructor_ = @Inject)
public class DeleteJobHandler {

  public static final String PATH_COMPONENT = "rpc/delete";

  final JobRunServiceComponent component;
  final RequestUtils requestUtils;

  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException, ServletException {

    JobRunId rootJobHandle = requestUtils.getRootPipelineId(req);

    StepExecutionComponent stepExecutionComponent =
      component.stepExecutionComponent(new StepExecutionModule(requestUtils.buildBackendFromRequest(req)));
    PipelineRunner pipelineRunner = stepExecutionComponent.pipelineRunner();

    try {
      //NOTE: previously this was async in Google's implementation; now sync
      pipelineRunner.deletePipelineRecords(rootJobHandle, true);
    } catch (NoSuchObjectException nsoe) {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    try {
      resp.getWriter().write("cancellation request was sent");
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }
}
