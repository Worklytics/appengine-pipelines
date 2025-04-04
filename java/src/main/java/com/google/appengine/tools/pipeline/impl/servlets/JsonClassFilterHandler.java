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

import static com.google.appengine.tools.pipeline.impl.util.JsonUtils.mapToJson;
import static java.util.Collections.singletonMap;

import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.PipelineRunner;

import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
@Singleton
@AllArgsConstructor(onConstructor_ = @Inject)
public class JsonClassFilterHandler {

  public static final String PATH_COMPONENT = "rpc/class_paths";

  final JobRunServiceComponent component;

  public void doGet(@SuppressWarnings("unused") HttpServletRequest req,
      HttpServletResponse resp) throws IOException {

    StepExecutionComponent stepExecutionComponent =
      component.stepExecutionComponent(new StepExecutionModule(req));
    PipelineRunner pipelineRunner = stepExecutionComponent.pipelineRunner();


    Set<String> pipelines = pipelineRunner.getRootPipelinesDisplayName();
    resp.getWriter().write(mapToJson(singletonMap("classPaths", pipelines)));
  }
}
