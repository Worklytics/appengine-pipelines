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

import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

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

  public static final String PATH_COMPONENT = "handleTask";

  public static final String TASK_NAME_REQUEST_HEADER = "X-CloudTasks-TaskName";
  public static final String TASK_RETRY_COUNT_HEADER = "X-CloudTasks-TaskRetryCount";
  public static final String TASK_QUEUE_NAME_HEADER = "X-CloudTasks-QueueName";

  //legacy - GAE tasks env
  @Deprecated
  public static final String TASK_NAME_REQUEST_LEGACY_HEADER = "X-AppEngine-TaskName";
  @Deprecated
  public static final String TASK_RETRY_COUNT_LEGACY_HEADER = "X-AppEngine-TaskRetryCount";
  @Deprecated
  public static final String TASK_QUEUE_NAME_LEGACY_HEADER = "X-AppEngine-QueueName";

  public static String handleTaskUrl() {
    return PipelineServlet.baseUrl() + PATH_COMPONENT;
  }

  public void doPost(HttpServletRequest req) throws ServletException {
    PipelineTask pipelineTask = reconstructTask(req);

    Integer retryCount = getTaskRetryCount(req);
    if (retryCount == null) {
      retryCount = -1;
    }

    try {
      StepExecutionComponent stepExecutionComponent =
        component.stepExecutionComponent(new StepExecutionModule(req));
      PipelineRunner pipelineRunner = stepExecutionComponent.pipelineRunner();

      pipelineRunner.processTask(pipelineTask);
    } catch (RuntimeException e) {
      logRetryMessage(log, pipelineTask, retryCount, e);
      throw new ServletException(e);
    }
  }

  String getTaskName(HttpServletRequest req) {
    return Optional.ofNullable(
        req.getHeader(TASK_NAME_REQUEST_HEADER))
      .orElseGet(() -> req.getHeader(TASK_NAME_REQUEST_LEGACY_HEADER));
  }

  String getQueueName(HttpServletRequest req) {
    return Optional.ofNullable(
        req.getHeader(TASK_QUEUE_NAME_HEADER))
      .orElseGet(() -> req.getHeader(TASK_QUEUE_NAME_LEGACY_HEADER));
  }

  Integer getTaskRetryCount(HttpServletRequest req) {
    return Stream.of(req.getHeader(TASK_RETRY_COUNT_HEADER),
      req.getHeader(TASK_RETRY_COUNT_LEGACY_HEADER))
      .filter(Objects::nonNull)
      .findFirst().map(Integer::parseInt).orElse(null);
  }

  private PipelineTask reconstructTask(HttpServletRequest request) {
    Properties properties = new Properties();
    Enumeration<?> paramNames = request.getParameterNames();
    while (paramNames.hasMoreElements()) {
      String paramName = (String) paramNames.nextElement();
      String paramValue = request.getParameter(paramName);
      properties.setProperty(paramName, paramValue);
    }
    String taskName = getTaskName(request);
    PipelineTask pipelineTask = PipelineTask.fromProperties(taskName, properties);
    pipelineTask.getQueueSettings().setDelayInSeconds(null);
    String queueName = getQueueName(request);
    if (queueName != null && !queueName.isEmpty()) {
      String onQueue = pipelineTask.getQueueSettings().getOnQueue();
       if (onQueue == null || onQueue.isEmpty()) {
         pipelineTask.getQueueSettings().setOnQueue(queueName);
       }
    }
    return pipelineTask;
  }

  @VisibleForTesting
  public static void logRetryMessage(Logger logger, PipelineTask pipelineTask, Integer retryCount, Exception e) {
    String message = "Will retry task: " + pipelineTask + ". retryCount=" + retryCount;
    if (e instanceof ConcurrentModificationException) {
      // Don't print stack trace in this case.
      logger.log(Level.WARNING, message + " " + e.getMessage());
    } else {
      logger.log(Level.WARNING, message, e);
    }
  }
}
