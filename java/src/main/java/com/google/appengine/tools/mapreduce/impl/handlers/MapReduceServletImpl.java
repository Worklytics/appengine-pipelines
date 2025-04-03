// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.di.JobRunServiceComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionComponent;
import com.google.appengine.tools.pipeline.di.StepExecutionModule;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.logging.Level;

import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.WORKER_PATH;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobHandler.*;
import static com.google.common.base.Preconditions.checkNotNull;

//TODO: not actually a servlet
@Log
@AllArgsConstructor(onConstructor_ = @Inject)
public class MapReduceServletImpl {

  JobRunServiceComponent component;
  StatusHandler statusHandler;
  RequestUtils requestUtils;

  static final String COMMAND_PATH = "command";

  /**
   * Handle GET http requests.
   */
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String handler = getHandler(request);
    if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      statusHandler.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      log.log(Level.SEVERE, String.format("Unknown MapReduce request handler: %s", handler));
    }
  }

  /**
   * Handle POST http requests.
   */
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String handler = getHandler(request);

    StepExecutionComponent stepExecutionComponent =
      component.stepExecutionComponent(new StepExecutionModule(request));

    if (handler.startsWith(CONTROLLER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      ShardedJobRunner shardedJobRunner = stepExecutionComponent.shardedJobRunner();
      shardedJobRunner.completeShard(getJobId(request), IncrementalTaskId.parse(request.getParameter(TASK_ID_PARAM)));
    } else if (handler.startsWith(WORKER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      ShardedJobRunner shardedJobRunner = stepExecutionComponent.shardedJobRunner();
      shardedJobRunner.runTask(
        getJobId(request),
        IncrementalTaskId.parse(checkNotNull(request.getParameter(TASK_ID_PARAM), "Null task id")),
        Integer.parseInt(request.getParameter(SEQUENCE_NUMBER_PARAM)),
        requestUtils.getRequestId(request)
        );
    } else if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      statusHandler.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      throw new RuntimeException(
          "Received an unknown MapReduce request handler. See logs for more detail.");
    }
  }

  private ShardedJobRunId getJobId(HttpServletRequest request) {
    return requestUtils.getParam(request, JOB_ID_PARAM).map(ShardedJobRunId::fromEncodedString)
      .orElseThrow(() -> new IllegalArgumentException("Missing " + JOB_ID_PARAM + " parameter"));
  }

  /**
   * Checks to ensure that the current request was sent via an AJAX request.
   *
   * If the request was not sent by an AJAX request, returns false, and sets
   * the response status code to 403. This protects against CSRF attacks against
   * AJAX only handlers.
   *
   * @return true if the request is a task queue request
   */
  private static boolean checkForAjax(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With"))) {
      log.log(
          Level.SEVERE, "Received unexpected non-XMLHttpRequest command. Possible CSRF attack.");
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
          "Received unexpected non-XMLHttpRequest command.");
      return false;
    }
    return true;
  }

  /**
   * Checks to ensure that the current request was sent via the task queue.
   *
   * If the request is not in the task queue, returns false, and sets the
   * response status code to 403. This protects against CSRF attacks against
   * task queue-only handlers.
   *
   * @return true if the request is a task queue request
   */
  private static boolean checkForTaskQueue(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    if (request.getHeader("X-AppEngine-QueueName") == null) {
      log.log(Level.SEVERE, "Received unexpected non-task queue request. Possible CSRF attack.");
      response.sendError(
          HttpServletResponse.SC_FORBIDDEN, "Received unexpected non-task queue request.");
      return false;
    }
    return true;
  }

  /**
   * Returns the handler portion of the URL path.
   *
   * For examples (for a servlet mapped as /foo/*):
   *   getHandler(https://www.google.com/foo/bar) -> bar
   *   getHandler(https://www.google.com/foo/bar/id) -> bar/id
   */
  private static String getHandler(HttpServletRequest request) {
    String pathInfo = request.getPathInfo();
    return pathInfo == null ? "" : pathInfo.substring(1);
  }
}