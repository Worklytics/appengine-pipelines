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

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.di.DaggerJobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.cloud.datastore.Key;
import com.google.appengine.tools.pipeline.util.Pair;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.Setter;


/**
 * Servlet that handles all requests for the Pipeline framework.
 * Dispatches all requests to {@link TaskHandler}, {@link JsonTreeHandler} or
 * {@link StaticContentHandler} as appropriate
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class PipelineServlet extends HttpServlet {

  public static final String BASE_URL_PROPERTY = "com.google.appengine.tools.pipeline.BASE_URL";

  @Setter(onMethod_ = @VisibleForTesting)
  JobRunServiceComponent component;

  @Override
  public void init() throws ServletException {
    super.init();
    if (this.component == null) {
      component = DaggerJobRunServiceComponent.create();
    }
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp);
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    Pair<String, RequestType> pair = parseRequestType(req);
    RequestType requestType = pair.getSecond();
    String path = pair.getFirst();
    switch (requestType) {
      case HANDLE_TASK:
        component.taskHandler().doPost(req);
        break;
      case GET_JSON:
        component.jsonTreeHandler().doGet(req, resp);
        break;
      case GET_JSON_LIST:
        component.jsonListHandler().doGet(req, resp);
        break;
      case GET_JSON_CLASS_FILTER:
        component.jsonClassFilterHandler().doGet(req, resp);
        break;
      case ABORT_JOB:
        component.abortJobHandler().doGet(req, resp);
        break;
      case DELETE_JOB:
        component.deleteJobHandler().doGet(req, resp);
        break;
      case HANDLE_STATIC:
        StaticContentHandler.doGet(resp, path);
        break;
      default:
        throw new ServletException("Unknown request type: " + requestType);
    }
  }

  /**
   * Returns the Pipeline's BASE URL.
   * This must match the URL in web.xml
   */
  public static String baseUrl() {
    String baseURL =  System.getProperty(BASE_URL_PROPERTY, "/_ah/pipeline/");
    if (!baseURL.endsWith("/")) {
      baseURL += "/";
    }
    return baseURL;
  }

  public static String makeViewerUrl(JobRunId pipelineRunId, JobRunId jobRunId) {
    return baseUrl() + "status.html?root=" + pipelineRunId.asEncodedString() + "#pipeline-" + jobRunId.asEncodedString();
  }

  public static String makeViewerUrl(JobRunId pipelineRunId, ShardedJobRunId shardedJobId) {
    //TODO: revisit this;
    return baseUrl() + "status.html?root=" + pipelineRunId.asEncodedString() + "#pipeline-" + shardedJobId.asEncodedString();
  }

  private enum RequestType {

    HANDLE_TASK(TaskHandler.PATH_COMPONENT),
    GET_JSON(JsonTreeHandler.PATH_COMPONENT),
    GET_JSON_LIST(JsonListHandler.PATH_COMPONENT),
    GET_JSON_CLASS_FILTER(JsonClassFilterHandler.PATH_COMPONENT),
    ABORT_JOB(AbortJobHandler.PATH_COMPONENT),
    DELETE_JOB(DeleteJobHandler.PATH_COMPONENT),
    HANDLE_STATIC("");

    private final String pathComponent;

    RequestType(String pathComponent) {
      this.pathComponent = pathComponent;
    }

    public boolean matches(String path) {
      return pathComponent.equals(path);
    }
  }

  private Pair<String, RequestType> parseRequestType(HttpServletRequest req) {
    String path = req.getPathInfo();
    path = path == null ? "" : path.substring(1); // Take off the leading '/'
    RequestType requestType = RequestType.HANDLE_STATIC;
    for (RequestType rt : RequestType.values()) {
      if (rt.matches(path)) {
        requestType = rt;
        break;
      }
    }
    return Pair.of(path, requestType);
  }
}
