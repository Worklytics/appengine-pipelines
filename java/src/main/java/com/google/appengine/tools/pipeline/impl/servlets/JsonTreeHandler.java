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

package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import lombok.AllArgsConstructor;

import java.io.IOException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
@Singleton
@AllArgsConstructor(onConstructor_ = @Inject)
public class JsonTreeHandler {

  final JobRunServiceComponent component;
  final RequestUtils requestUtils;

  public static final String PATH_COMPONENT = "rpc/tree";
  private static final String ROOT_PIPELINE_ID = "root_pipeline_id";

  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException {

    String rootJobHandle = req.getParameter(ROOT_PIPELINE_ID);
    if (null == rootJobHandle) {
      throw new ServletException(ROOT_PIPELINE_ID + " parameter not found.");
    }
    try {

      StepExecutionComponent stepExecutionComponent =
        component.stepExecutionComponent(new StepExecutionModule(requestUtils.buildBackendFromRequest(req)));
      PipelineRunner pipelineRunner = stepExecutionComponent.pipelineRunner();

      JobRecord jobInfo;
      try {
        jobInfo = pipelineRunner.getJob(rootJobHandle);
      } catch (NoSuchObjectException nsoe) {
        resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        return;
      }
      String rootJobKey = jobInfo.getRootJobKey().getName();
      if (!rootJobKey.equals(rootJobHandle)) {
        resp.addHeader(ROOT_PIPELINE_ID, rootJobKey);
        resp.sendError(449, rootJobKey);
        return;
      }
      PipelineObjects pipelineObjects = pipelineRunner.queryFullPipeline(rootJobKey);
      String asJson = JsonGenerator.pipelineObjectsToJson(pipelineObjects);
      // TODO(user): Temporary until we support abort/delete in Python
      resp.addHeader("Pipeline-Lang", "Java");
      resp.getWriter().write(asJson);
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }
}
