/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce.impl.handlers;

import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceServlet;

import com.google.appengine.tools.mapreduce.PipelineSetupExtensions;
import com.google.appengine.tools.pipeline.TestUtils;
import com.google.appengine.tools.pipeline.TestingTaskQueueCallback;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.cloud.datastore.Datastore;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Tests MapReduceServlet
 *
 */
@PipelineSetupExtensions
public class MapReduceServletTest{

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalTaskQueueTestConfig(), new LocalMemcacheServiceTestConfig());

  private MapReduceServlet servlet;

  @BeforeEach
  public void setUp() throws Exception {
    helper.setUp();
    servlet = new MapReduceServlet();
    servlet.init();
  }

  @BeforeEach
  public void setup(PipelineManager pipelineManager) {
    TestingTaskQueueCallback.pipelineManager = pipelineManager;
  }

  @Getter @Setter(onMethod_ = @BeforeEach)
  Datastore datastore;

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testBailsOnBadHandler() throws Exception {
    HttpServletRequest request = createMockRequest(getDatastore(), "fizzle", true, true);
    HttpServletResponse response = mock(HttpServletResponse.class);

    try {
      servlet.doPost(request, response);
      fail("Should have thrown RuntimeException");
    } catch (RuntimeException e) {
      // Pass
    }
  }


  /**
   * PrintWriter implements locks, which don't get initialized by the constructor using a stream
   */
  public class NonLockingPrintWriter extends PrintWriter {
    private final Writer out;

    public NonLockingPrintWriter(Writer out) {
      super(out);
      this.out = out;
    }

    @SneakyThrows
    @Override
    public void write(int c) {
        out.write(c);
    }

    @SneakyThrows
    @Override
    public void write(char[] buf, int off, int len) {
      out.write(buf, off, len);
    }

    @SneakyThrows
    @Override
    public void write(String s, int off, int len) {
      out.write(s, off, len);
    }

    @SneakyThrows
    @Override
    public void flush() {
      out.flush();
    }

    @SneakyThrows
    @Override
    public void close() {
      out.close();
    }
  }

  @Test
  public void ignoredTestCommandError() throws Exception {
    HttpServletRequest request = createMockRequest(getDatastore(),
        MapReduceServletImpl.COMMAND_PATH + "/" + StatusHandler.GET_JOB_DETAIL_PATH, false, true);
    when(request.getMethod()).thenReturn("GET");
    HttpServletResponse response = mock(HttpServletResponse.class);

    StringWriter stringWriter = new StringWriter();

    PrintWriter responseWriter = spy(new NonLockingPrintWriter(stringWriter));
    // This method can't actually throw this exception, but that's not
    // important to the test.

    //throw a fake exception, so that MapReduceServletImpl will have to handle an exception
    when(request.getParameter("mapreduce_id")).thenThrow(new RuntimeException("blargh"));
    response.setContentType("application/json");
    when(response.getWriter()).thenReturn(responseWriter);

    servlet.doPost(request, response);

    verify(responseWriter, times(1)).flush();
    assertEquals("{\"error_message\":\"Full stack trace is available in the server logs. Message: blargh\",\"error_class\":\"java.lang.RuntimeException\"}", stringWriter.toString());
  }

  @Test
  public void testControllerCSRF() throws Exception {
    // Send it as an AJAX request but not a task queue request - should be denied.
    HttpServletRequest request = createMockRequest(getDatastore(), CONTROLLER_PATH, false, true);
    HttpServletResponse response = mock(HttpServletResponse.class);

    servlet.doPost(request, response);

    verify(response, times(1)).sendError(eq(403), eq("Received unexpected non-task queue request."));
  }

  @Test
  public void testGetJobDetailCSRF() throws Exception {
    // Send it as a task queue request but not an ajax request - should be denied.
    HttpServletRequest request = createMockRequest(getDatastore(),
      MapReduceServletImpl.COMMAND_PATH + "/" + StatusHandler.GET_JOB_DETAIL_PATH, true, false);
    when(request.getMethod()).thenReturn("POST");

    HttpServletResponse response = mock(HttpServletResponse.class);

    servlet.doGet(request, response);

    verify(response, times(1)).sendError(eq(403), eq("Received unexpected non-XMLHttpRequest command."));
  }

  @Test
  public void testStaticResources_jQuery() throws Exception {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    resp.setContentType("text/javascript");
    resp.setHeader("Cache-Control", "public; max-age=300");
    ServletOutputStream sos = mock(ServletOutputStream.class);
    when(resp.getOutputStream()).thenReturn(sos);

    MapReduceServletImpl.handleStaticResources("jquery.js", resp);

    verify(resp, atLeastOnce()).getOutputStream();
    verify(sos, atLeastOnce()).write(any(), eq(0), anyInt());
    verify(sos, atLeastOnce()).flush(); //actually needed?
  }

  @Test
  public void testStaticResources_status() throws Exception {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    resp.sendRedirect("/_ah/pipeline/list?class_path=" + MapReduceJob.class.getName());
    MapReduceServletImpl.handleStaticResources("status", resp);
  }

  private static HttpServletRequest createMockRequest(Datastore datastore,
      String handler, boolean taskQueueRequest, boolean ajaxRequest) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    if (taskQueueRequest) {
      when(request.getHeader(eq("X-AppEngine-QueueName")))
          .thenReturn("default");
    } else {
      when(request.getHeader(eq("X-AppEngine-QueueName")))
          .thenReturn(null);
    }
    if (ajaxRequest) {
      when(request.getHeader(eq("X-Requested-With")))
          .thenReturn("XMLHttpRequest");
    } else {
      when(request.getHeader(eq("X-Requested-With")))
          .thenReturn(null);
    }
    when(request.getPathInfo())
        .thenReturn("/" + handler);

    TestUtils.addDatastoreHeadersToRequest(request, datastore.getOptions());

    return request;
  }
}
