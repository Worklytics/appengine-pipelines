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
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceServlet;

import com.google.appengine.tools.mapreduce.PipelineSetupExtensions;
import com.google.appengine.tools.mapreduce.di.DaggerDefaultMapReduceContainer;
import com.google.appengine.tools.pipeline.impl.util.DIUtil;
import com.google.cloud.datastore.Datastore;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

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

    // still using default module, which builds pipeline options with defualts, which is not good
    DIUtil.overrideComponentInstanceForTests(DaggerDefaultMapReduceContainer.class, DaggerDefaultMapReduceContainer.create());
    DIUtil.inject(servlet);
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testBailsOnBadHandler() throws Exception {
    HttpServletRequest request = createMockRequest("fizzle", true, true);
    HttpServletResponse response = createMock(HttpServletResponse.class);
    replay(request, response);
    try {
      servlet.doPost(request, response);
      fail("Should have thrown RuntimeException");
    } catch (RuntimeException e) {
      // Pass
    }
    verify(request, response);
  }

  @Disabled
  @Test
  //this stupid test fails because it's coupled to exact JSON serialization implementation
  // (eg, calling to PrintWriter.write, with chars/ints/strings as expected)
  public void ignoredTestCommandError() throws Exception {
    HttpServletRequest request = createMockRequest(
        MapReduceServletImpl.COMMAND_PATH + "/" + StatusHandler.GET_JOB_DETAIL_PATH, false, true);
    expect(request.getMethod()).andReturn("GET").anyTimes();
    HttpServletResponse response = createMock(HttpServletResponse.class);

    //TODO: this should be a spy or something, but Easymock doesn't support spys. result is very cryptic errors, due
    // to single character-level inconsistencies in output
    PrintWriter responseWriter = createMock(PrintWriter.class);
    responseWriter.write('{');
    responseWriter.write("\"error_class\"");
    responseWriter.write(':');
    responseWriter.write("\"java.lang.RuntimeException\"");
    responseWriter.write(',');
    responseWriter.write("\"error_message\"");
    responseWriter.write(':');
    responseWriter.write("\"Full stack trace is available in the server logs. "
        + "Message: blargh\"");
    responseWriter.write('}');
    responseWriter.flush();
    // This method can't actually throw this exception, but that's not
    // important to the test.

    //throw a fake exception, so that MapReduceServletImpl will have to handle an exception
    expect(request.getParameter("mapreduce_id")).andThrow(new RuntimeException("blargh"));
    response.setContentType("application/json");
    expect(response.getWriter()).andReturn(responseWriter).anyTimes();
    replay(request, response, responseWriter);
    servlet.doPost(request, response);
    verify(request, response, responseWriter);
  }

  @Test
  public void testControllerCSRF() throws Exception {
    // Send it as an AJAX request but not a task queue request - should be denied.
    HttpServletRequest request = createMockRequest(CONTROLLER_PATH, false, true);
    HttpServletResponse response = createMock(HttpServletResponse.class);
    response.sendError(403, "Received unexpected non-task queue request.");
    replay(request, response);
    servlet.doPost(request, response);
    verify(request, response);
  }

  @Test
  public void testGetJobDetailCSRF() throws Exception {
    // Send it as a task queue request but not an ajax request - should be denied.
    HttpServletRequest request = createMockRequest(
        MapReduceServletImpl.COMMAND_PATH + "/" + StatusHandler.GET_JOB_DETAIL_PATH, true, false);
    expect(request.getMethod()).andReturn("POST").anyTimes();

    HttpServletResponse response = createMock(HttpServletResponse.class);

    // Set before error and last one wins, so this is harmless.
    response.setContentType("application/json");
    EasyMock.expectLastCall().anyTimes();

    response.sendError(403, "Received unexpected non-XMLHttpRequest command.");
    replay(request, response);
    servlet.doGet(request, response);
    verify(request, response);
  }

  @Test
  public void testStaticResources_jQuery() throws Exception {
    HttpServletResponse resp = createMock(HttpServletResponse.class);
    resp.setContentType("text/javascript");
    resp.setHeader("Cache-Control", "public; max-age=300");
    ServletOutputStream sos = createMock(ServletOutputStream.class);
    expect(resp.getOutputStream()).andReturn(sos);
    sos.write((byte[]) EasyMock.anyObject(), EasyMock.eq(0), EasyMock.anyInt());
    EasyMock.expectLastCall().atLeastOnce();
    sos.flush();
    EasyMock.expectLastCall().anyTimes();
    replay(resp, sos);
    MapReduceServletImpl.handleStaticResources("jquery.js", resp);
    verify(resp, sos);
  }

  @Test
  public void testStaticResources_status() throws Exception {
    HttpServletResponse resp = createMock(HttpServletResponse.class);
    resp.sendRedirect("/_ah/pipeline/list?class_path=" + MapReduceJob.class.getName());
    replay(resp);
    MapReduceServletImpl.handleStaticResources("status", resp);
    verify(resp);
  }

  private static HttpServletRequest createMockRequest(
      String handler, boolean taskQueueRequest, boolean ajaxRequest) {
    HttpServletRequest request = createMock(HttpServletRequest.class);
    if (taskQueueRequest) {
      expect(request.getHeader("X-AppEngine-QueueName"))
          .andReturn("default")
          .anyTimes();
    } else {
      expect(request.getHeader("X-AppEngine-QueueName"))
          .andReturn(null)
          .anyTimes();
    }
    if (ajaxRequest) {
      expect(request.getHeader("X-Requested-With"))
          .andReturn("XMLHttpRequest")
          .anyTimes();
    } else {
      expect(request.getHeader("X-Requested-With"))
          .andReturn(null)
          .anyTimes();
    }
    expect(request.getPathInfo())
        .andReturn("/" + handler)
        .anyTimes();
    return request;
  }
}
