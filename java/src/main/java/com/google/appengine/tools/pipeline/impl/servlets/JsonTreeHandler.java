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
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesServiceImpl;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import lombok.AllArgsConstructor;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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

    JobRunId rootJobHandle = requestUtils.getRootPipelineId(req);
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
      JobRunId rootJobRunId = JobRunId.of(jobInfo.getRootJobKey());
      if (!rootJobRunId.equals(rootJobHandle)) {
        //in effect, value passed to servlet for root_pipeline_id is not in fact the id of a root job of a pipeline
        resp.addHeader(ROOT_PIPELINE_ID, rootJobRunId.asEncodedString());
        resp.sendError(449, "parsed root_pipeline_id (" + rootJobHandle + ") has JobInfo from different root job : "+ rootJobRunId);
        return;
      }
      PipelineObjects pipelineObjects = pipelineRunner.queryFullPipeline(rootJobRunId);
      String asJson = JsonGenerator.pipelineObjectsToJson(pipelineObjects);
      // TODO(user): Temporary until we support abort/delete in Python
      resp.addHeader("Pipeline-Lang", "Java");
      resp.getWriter().write(asJson);
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }
}
