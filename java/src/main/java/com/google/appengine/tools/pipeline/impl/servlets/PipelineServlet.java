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

import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.Key;
import com.google.appengine.tools.pipeline.util.Pair;
import lombok.SneakyThrows;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that handles all requests for the Pipeline framework.
 * Dispatches all requests to {@link TaskHandler}, {@link JsonTreeHandler} or
 * {@link StaticContentHandler} as appropriate
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
@SuppressWarnings("serial")
public class PipelineServlet extends HttpServlet {

  public static final String BASE_URL_PROPERTY = "com.google.appengine.tools.pipeline.BASE_URL";

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

  public static String makeViewerUrl(Key rootJobKey, Key jobKey) {
    return baseUrl() + "status.html?root=" + rootJobKey.toUrlSafe() + "#pipeline-" + jobKey.toUrlSafe();
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

  transient AbortJobHandler abortJobHandler;
  transient DeleteJobHandler deleteJobHandler;
  transient JsonClassFilterHandler jsonClassFilterHandler;
  transient JsonListHandler jsonListHandler;
  transient JsonTreeHandler jsonTreeHandler;
  transient TaskHandler taskHandler;

  @SneakyThrows
  @Override
  public void init() throws ServletException {
    super.init();
    //TODO: coupled to GAE; could we pull this via ServletConfig somehow, to make more flexible?
    PipelineManager pipelineManager =
      new PipelineManager(SystemProperty.applicationId.get(), GoogleCredentials.getApplicationDefault());
    abortJobHandler = new AbortJobHandler(pipelineManager);
    deleteJobHandler = new DeleteJobHandler(pipelineManager);
    jsonClassFilterHandler = new JsonClassFilterHandler(pipelineManager);
    jsonListHandler = new JsonListHandler(pipelineManager);
    jsonTreeHandler = new JsonTreeHandler(pipelineManager);
    taskHandler = new TaskHandler(pipelineManager);
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
        taskHandler.doPost(req);
        break;
      case GET_JSON:
        jsonTreeHandler.doGet(req, resp);
        break;
      case GET_JSON_LIST:
        jsonListHandler.doGet(req, resp);
        break;
      case GET_JSON_CLASS_FILTER:
        jsonClassFilterHandler.doGet(req, resp);
        break;
      case ABORT_JOB:
        abortJobHandler.doGet(req, resp);
        break;
      case DELETE_JOB:
        deleteJobHandler.doGet(req, resp);
        break;
      case HANDLE_STATIC:
        StaticContentHandler.doGet(resp, path);
        break;
      default:
        throw new ServletException("Unknown request type: " + requestType);
    }
  }
}
