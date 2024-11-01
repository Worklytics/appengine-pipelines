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

import static com.google.appengine.tools.pipeline.TestUtils.assertEqualsIgnoreWhitespace;
import static com.google.appengine.tools.pipeline.TestUtils.waitForJobToComplete;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.appengine.tools.pipeline.*;

import com.google.cloud.datastore.Datastore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Test for {@link JsonClassFilterHandler}.
 */
public class JsonClassFilterHandlerTest extends PipelineTest {

  private HttpServletRequest request;
  private HttpServletResponse response;

  private final StringWriter output = new StringWriter();
  @SuppressWarnings("serial")
  private static class Main1Job extends Job0<String> {

    @Override
    public Value<String> run() {
      FutureValue<String> v1 = futureCall(new StrJob<>(), immediate("hello"));
      FutureValue<String> v2 = futureCall(new StrJob<>(), immediate(" world"));
      return futureCall(new ConcatJob(), v1, v2);
    }
  }

  @SuppressWarnings("serial")
  private static class Main2Job extends Job0<String> {

    private final boolean shouldThrow;

    public Main2Job(boolean shouldThrow) {
      this.shouldThrow = shouldThrow;
    }

    @Override
    public Value<String> run() {
      if (shouldThrow) {
        throw new RuntimeException("bla");
      }
      FutureValue<String> v1 = futureCall(new StrJob<>(), immediate("hi"));
      FutureValue<String> v2 = futureCall(new StrJob<>(), immediate(" there"));
      return futureCall(new ConcatJob(), v1, v2);
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(Throwable t) {
      return immediate(t.getMessage());
    }
  }

  @SuppressWarnings("serial")
  private static class ConcatJob extends Job2<String, String, String> {

    @Override
    public Value<String> run(String value1, String value2) {
      return immediate(value1 + value2);
    }
  }

  @SuppressWarnings("serial")
  private static class StrJob<T extends Serializable> extends Job1<String, T> {

    @Override
    public Value<String> run(T obj) {
      return immediate(obj == null ? "null" : obj.toString());
    }
  }

  @BeforeEach
  public void setUp(Datastore datastore) throws Exception {
    handler = getComponent().jsonClassFilterHandler();

    response = mock(HttpServletResponse.class);
    request = mock(HttpServletRequest.class);
    TestUtils.addDatastoreHeadersToRequest(request, datastore.getOptions());
    when(request.getParameter(JsonListHandler.CLASS_FILTER_PARAMETER)).thenReturn(null);
    when(request.getParameter(JsonListHandler.CURSOR_PARAMETER)).thenReturn(null);
    when(request.getParameter(JsonListHandler.LIMIT_PARAMETER)).thenReturn(null);
    when(response.getWriter()).thenReturn(new PrintWriter(output));
  }

  JsonClassFilterHandler handler;

  @Test
  public void testHandlerNoResults() throws Exception {
    handler.doGet(request, response);
    assertEqualsIgnoreWhitespace("{\"classPaths\": []}", output.toString());
  }

  @Test
  public void testHandlerWithResults() throws Exception {
    JobRunId pipelineId1 = pipelineService.startNewPipeline(new Main1Job());
    JobRunId pipelineId2 = pipelineService.startNewPipeline(new Main2Job(false));
    JobRunId pipelineId3 = pipelineService.startNewPipeline(new Main2Job(true),
        new JobSetting.BackoffSeconds(0), new JobSetting.MaxAttempts(2));

    String helloWorld = waitForJobToComplete(pipelineService, pipelineId1);
    assertEquals("hello world", helloWorld);
    String hiThere = waitForJobToComplete(pipelineService, pipelineId2);
    assertEquals("hi there", hiThere);
    String bla = waitForJobToComplete(pipelineService, pipelineId3);

    assertEquals("bla", bla);

    handler.doGet(request, response);
    System.out.println(output.toString());
    String expected = "{\"classPaths\": [\n"
        + "  \"" + Main1Job.class.getName() + "\",\n"
        + "  \"" + Main2Job.class.getName() + "\"\n"
        + "]}";
    assertEqualsIgnoreWhitespace(expected, output.toString());
  }
}
