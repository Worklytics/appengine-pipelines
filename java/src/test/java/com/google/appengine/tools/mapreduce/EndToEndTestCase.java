// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.HeaderWrapper;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobHandler;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.pipeline.PipelineOrchestrator;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.TestUtils;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.test.CloudStorageExtensions;
import com.google.appengine.tools.test.PipelineSetupExtensions;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.storage.Storage;
import com.google.common.base.CharMatcher;

import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@CloudStorageExtensions
@PipelineSetupExtensions
public abstract class EndToEndTestCase {

  private static final Logger logger = Logger.getLogger(EndToEndTestCase.class.getName());

  @Setter(onMethod_ = @BeforeEach)
  private LocalTaskQueue taskQueue;

  @Getter @Setter(onMethod_ = @BeforeEach)
  Datastore datastore;

  @Getter @Setter(onMethod_ = @BeforeEach)
  PipelineService pipelineService;

  @Getter @Setter(onMethod_ = @BeforeEach)
  PipelineRunner pipelineRunner;

  @Getter @Setter(onMethod_ = @BeforeEach)
  PipelineOrchestrator pipelineOrchestrator;

  @Getter @Setter(onMethod_ = @BeforeEach)
  Storage storage;


  // will this magically have right context?
  private PipelineServlet pipelineServlet = new PipelineServlet();
  private MapReduceServlet mrServlet = new MapReduceServlet();

  @BeforeEach
  public void setUp() throws Exception {
    pipelineServlet.init();
    mrServlet.init();
  }


  public ShardedJobRunId shardedJobId(String jobId) {
      return ShardedJobRunId.of(
        getDatastore().getOptions().getProjectId(),
        getDatastore().getOptions().getDatabaseId(),
        getDatastore().getOptions().getNamespace(),
        jobId);
  }

  protected List<QueueStateInfo.TaskStateInfo> getTasks() {
    return getTasks("default");
  }

  protected List<QueueStateInfo.TaskStateInfo> getTasks(String queueName) {
    return taskQueue.getQueueStateInfo().get(queueName).getTaskInfo();
  }

  protected void executeTask(String queueName, QueueStateInfo.TaskStateInfo taskStateInfo)
      throws Exception {
    logger.fine("Executing task " + taskStateInfo.getTaskName()
        + " with URL " + taskStateInfo.getUrl());
    // Hack to allow for deferred tasks. Exploits knowing how they work.
    if (taskStateInfo.getUrl().endsWith("__deferred__")) {
      ObjectInputStream oin =
          new ObjectInputStream(new ByteArrayInputStream(taskStateInfo.getBodyAsBytes()));
      Runnable object = (Runnable) oin.readObject();
      object.run();
      return;
    }
    HttpServletRequest request = createMock(HttpServletRequest.class);
    HttpServletResponse response = createMock(HttpServletResponse.class);

    String pathInfo = taskStateInfo.getUrl();
    if (pathInfo.startsWith("/")) {
      int skipFrom = pathInfo.startsWith("/_ah/") ? 5 : 1;
      pathInfo = pathInfo.substring(pathInfo.indexOf('/', skipFrom));
    } else {
      pathInfo = "/" + pathInfo;
    }
    expect(request.getPathInfo()).andReturn(pathInfo).anyTimes();
    expect(request.getHeader("X-AppEngine-QueueName")).andReturn(queueName).anyTimes();
    expect(request.getHeader("X-AppEngine-TaskName")).andReturn(taskStateInfo.getTaskName())
        .anyTimes();
    // Pipeline looks at this header but uses the value only for diagnostic messages
    expect(request.getIntHeader(TaskHandler.TASK_RETRY_COUNT_HEADER)).andReturn(-1).anyTimes();
    for (HeaderWrapper header : taskStateInfo.getHeaders()) {
      int value = parseAsQuotedInt(header.getValue());
      expect(request.getIntHeader(header.getKey())).andReturn(value).anyTimes();
      logger.fine("header: " + header.getKey() + "=" + header.getValue());
      expect(request.getHeader(header.getKey())).andReturn(header.getValue()).anyTimes();
    }

    Map<String, String> parameters = decodeParameters(taskStateInfo.getBody());
    for (String name : parameters.keySet()) {
      expect(request.getParameter(name)).andReturn(parameters.get(name)).anyTimes();
    }
    expect(request.getParameterNames()).andReturn(Collections.enumeration(parameters.keySet()))
        .anyTimes();

    TestUtils.addDatastoreHeadersToRequestEasymock(request, datastore.getOptions());

    replay(request, response);

    if (taskStateInfo.getMethod().equals("POST")) {
      if (taskStateInfo.getUrl().startsWith(PipelineServlet.baseUrl())) {
        pipelineServlet.doPost(request, response);
      } else {
        mrServlet.doPost(request, response);
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private int parseAsQuotedInt(String str) {
    try {
      return Integer.parseInt(CharMatcher.is('"').trimFrom(str));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  protected void executeTasksUntilEmpty() throws Exception {
    executeTasksUntilEmpty("default");
  }

  protected void executeTasksUntilEmpty(String queueName) throws Exception {
    while (true) {
      // We have to re-acquire task list every time, because local implementation returns a copy.
      List<QueueStateInfo.TaskStateInfo> taskInfo =
          taskQueue.getQueueStateInfo().get(queueName).getTaskInfo();
      if (taskInfo.isEmpty()) {
        break;
      }
      QueueStateInfo.TaskStateInfo taskStateInfo = taskInfo.get(0);
      taskQueue.deleteTask(queueName, taskStateInfo.getTaskName());
      executeTask(queueName, taskStateInfo);
    }
  }

  protected TaskStateInfo grabNextTaskFromQueue(String queueName) {
    List<TaskStateInfo> taskInfo = getTasks(queueName);
    assertFalse(taskInfo.isEmpty());
    TaskStateInfo taskStateInfo = taskInfo.get(0);
    taskQueue.deleteTask(queueName, taskStateInfo.getTaskName());
    return taskStateInfo;
  }

  // Sadly there's no way to parse query string with JDK. This is a good enough approximation.
  private static Map<String, String> decodeParameters(String requestBody)
      throws UnsupportedEncodingException {
    Map<String, String> result = new HashMap<>();

    String[] params = requestBody.split("&");
    for (String param : params) {
      String[] pair = param.split("=");
      String name = pair[0];
      String value = URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
      if (result.containsKey(name)) {
        throw new IllegalArgumentException("Duplicate parameter: " + requestBody);
      }
      result.put(name, value);
    }

    return result;
  }

  protected IncrementalTaskId getTaskId(TaskStateInfo taskStateInfo) throws UnsupportedEncodingException {
    return IncrementalTaskId.parse(decodeParameters(taskStateInfo.getBody()).get(ShardedJobHandler.TASK_ID_PARAM));
  }
}
