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

import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.PipelineOrchestrator;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import lombok.RequiredArgsConstructor;

import java.io.IOException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * @author ozarov@google.com (Arie Ozarov)
 */
@RequiredArgsConstructor
public class AbortJobHandler {

  public static final String PATH_COMPONENT = "rpc/abort";
  private static final String ROOT_PIPELINE_ID = "root_pipeline_id";

  private final PipelineOrchestrator pipelineManager;

  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException, ServletException {
    String rootJobHandle = req.getParameter(ROOT_PIPELINE_ID);
    if (null == rootJobHandle) {
      throw new ServletException(ROOT_PIPELINE_ID + " parameter not found.");
    }
    try {
      pipelineManager.cancelJob(rootJobHandle);
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
