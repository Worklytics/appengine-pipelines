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

import com.google.appengine.tools.pipeline.PipelineRunner;

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.Set;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import javax.inject.Inject;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class JsonClassFilterHandler {

  public static final String PATH_COMPONENT = "rpc/class_paths";

  private final PipelineRunner pipelineManager;

  public void doGet(@SuppressWarnings("unused") HttpServletRequest req,
      HttpServletResponse resp) throws IOException {
    Set<String> pipelines = pipelineManager.getRootPipelinesDisplayName();
    resp.getWriter().write(mapToJson(singletonMap("classPaths", pipelines)));
  }
}
