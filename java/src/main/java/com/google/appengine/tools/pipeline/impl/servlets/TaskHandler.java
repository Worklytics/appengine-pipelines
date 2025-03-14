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
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesServiceImpl;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.apphosting.api.ApiProxy;
import lombok.AllArgsConstructor;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * A ServletHelper that handles all requests from the task queue.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
@Singleton
@Log
@AllArgsConstructor(onConstructor_ = @Inject)
public class TaskHandler {

  final JobRunServiceComponent component;
  final RequestUtils requestUtils;

  public static final String PATH_COMPONENT = "handleTask";
  public static final String TASK_NAME_REQUEST_HEADER = "X-AppEngine-TaskName";
  public static final String TASK_RETRY_COUNT_HEADER = "X-AppEngine-TaskRetryCount";
  public static final String TASK_QUEUE_NAME_HEADER = "X-AppEngine-QueueName";

  public static String handleTaskUrl() {
    return PipelineServlet.baseUrl() + PATH_COMPONENT;
  }


  public void doPost(HttpServletRequest req) throws ServletException {
    Task task = reconstructTask(req);

    int retryCount;
    try {
      retryCount = req.getIntHeader(TASK_RETRY_COUNT_HEADER);
    } catch (NumberFormatException e) {
      retryCount = -1;
    }
    try {
      StepExecutionComponent stepExecutionComponent =
        component.stepExecutionComponent(new StepExecutionModule(requestUtils.buildBackendFromRequest(req)));
      PipelineRunner pipelineRunner = stepExecutionComponent.pipelineRunner();

      pipelineRunner.processTask(task);
    } catch (RuntimeException e) {
      StringUtils.logRetryMessage(log, task, retryCount, e);
      throw new ServletException(e);
    }
  }

  private Task reconstructTask(HttpServletRequest request) {
    Properties properties = new Properties();
    Enumeration<?> paramNames = request.getParameterNames();
    while (paramNames.hasMoreElements()) {
      String paramName = (String) paramNames.nextElement();
      String paramValue = request.getParameter(paramName);
      properties.setProperty(paramName, paramValue);
    }
    String taskName = request.getHeader(TASK_NAME_REQUEST_HEADER);
    Task task = Task.fromProperties(taskName, properties);
    task.getQueueSettings().setDelayInSeconds(null);
    String queueName = request.getHeader(TASK_QUEUE_NAME_HEADER);
    if (queueName != null && !queueName.isEmpty()) {
      String onQueue = task.getQueueSettings().getOnQueue();
       if (onQueue == null || onQueue.isEmpty()) {
         task.getQueueSettings().setOnQueue(queueName);
       }
       Map<String, Object> attributes = ApiProxy.getCurrentEnvironment().getAttributes();
       attributes.put(TASK_QUEUE_NAME_HEADER, queueName);
    }
    return task;
  }
}
