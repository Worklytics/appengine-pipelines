// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.HeaderWrapper;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobHandler;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.pipeline.PipelineOrchestrator;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.TestUtils;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.testutil.JobRunServiceTestComponent;
import com.google.cloud.datastore.Datastore;
import com.google.common.base.CharMatcher;

import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.AfterEach;
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

@PipelineSetupExtensions
public abstract class EndToEndTestCase {

  private static final Logger logger = Logger.getLogger(EndToEndTestCase.class.getName());

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalTaskQueueTestConfig().setDisableAutoTaskExecution(true));
  private LocalTaskQueue taskQueue;

  /** Implement in sub-classes to set system environment properties for tests. */
  protected Map<String, String> getEnvAttributes() throws Exception {
    return null;
  }

  @Getter @Setter(onMethod_ = @BeforeEach)
  Datastore datastore;

  @Getter @Setter(onMethod_ = @BeforeEach)
  PipelineService pipelineService;

  @Getter @Setter(onMethod_ = @BeforeEach)
  PipelineRunner pipelineRunner;

  @Getter @Setter(onMethod_ = @BeforeEach)
  PipelineOrchestrator pipelineOrchestrator;

  // will this magically have right context?
  //TODO: get these from the component ? how
  private PipelineServlet pipelineServlet = new PipelineServlet();
  private MapReduceServlet mrServlet = new MapReduceServlet();

  @Getter
  private CloudStorageIntegrationTestHelper storageTestHelper;

  @BeforeEach
  public void setUp(JobRunServiceTestComponent component) throws Exception {
    helper.setUp();
    Map<String, String> envAttributes = getEnvAttributes();
    if (envAttributes != null) {
      LocalServiceTestHelper.getApiProxyLocal().appendProperties(envAttributes);
    }
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();
    // Creating files is not allowed in some test execution environments, so don't.
    storageTestHelper = new CloudStorageIntegrationTestHelper();
    storageTestHelper.setUp();

    pipelineServlet.setComponent(component);
    mrServlet.setComponent(component);
    pipelineServlet.init();
    mrServlet.init();
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
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
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    String pathInfo = taskStateInfo.getUrl();
    if (pathInfo.startsWith("/")) {
      int skipFrom = pathInfo.startsWith("/_ah/") ? 5 : 1;
      pathInfo = pathInfo.substring(pathInfo.indexOf('/', skipFrom));
    } else {
      pathInfo = "/" + pathInfo;
    }
    when(request.getPathInfo()).thenReturn(pathInfo);
    when(request.getHeader(eq("X-AppEngine-QueueName"))).thenReturn(queueName);
    when(request.getHeader(eq("X-AppEngine-TaskName"))).thenReturn(taskStateInfo.getTaskName());
    // Pipeline looks at this header but uses the value only for diagnostic messages
    when(request.getIntHeader(eq(TaskHandler.TASK_RETRY_COUNT_HEADER))).thenReturn(-1);
    for (HeaderWrapper header : taskStateInfo.getHeaders()) {
      int value = parseAsQuotedInt(header.getValue());
      when(request.getIntHeader(header.getKey())).thenReturn(value);
      logger.fine("header: " + header.getKey() + "=" + header.getValue());
      when(request.getHeader(eq(header.getKey()))).thenReturn(header.getValue());
    }

    Map<String, String> parameters = decodeParameters(taskStateInfo.getBody());
    for (String name : parameters.keySet()) {
      when(request.getParameter(eq(name))).thenReturn(parameters.get(name));
    }
    when(request.getParameterNames()).thenReturn(Collections.enumeration(parameters.keySet()));

    TestUtils.addDatastoreHeadersToRequest(request, datastore.getOptions());

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
