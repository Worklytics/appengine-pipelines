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
import com.google.appengine.tools.pipeline.impl.util.JsonUtils;

import com.google.cloud.datastore.Datastore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * Test for {@link JsonListHandlerTest}.
 */
public class JsonListHandlerTest extends PipelineTest {


  private HttpServletRequest request;
 private HttpServletResponse response;
  private StringWriter output = new StringWriter();

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

  JsonListHandler jsonListHandler;

  @BeforeEach
  public void setUp(Datastore datastore) throws Exception {
    jsonListHandler = getComponent().jsonListHandler();

    request = mock(HttpServletRequest.class);
    TestUtils.addDatastoreHeadersToRequest(request, datastore.getOptions());
    when(request.getParameter(JsonListHandler.CLASS_FILTER_PARAMETER)).thenReturn(null);
    when(request.getParameter(JsonListHandler.CURSOR_PARAMETER)).thenReturn(null);
    when(request.getParameter(JsonListHandler.LIMIT_PARAMETER)).thenReturn(null);

    output = new StringWriter();
    response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(output));
  }

  @Test
  public void testHandlerNoResults() throws Exception {
    jsonListHandler.doGet(request, response);
    assertEqualsIgnoreWhitespace("{\"pipelines\": []}", output.toString());
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
    jsonListHandler.doGet(request, response);

    String rawOutput = output.toString();

    Map<String, Object> results = (Map<String, Object>) JsonUtils.fromJson(rawOutput);
    assertEquals(1, results.size());
    List<Map<String, Object>> pipelines = (List<Map<String, Object>>) results.get("pipelines");
    assertEquals(3, pipelines.size());
    Map<String, String> pipelineIdToClass = new HashMap<>();
    for (Map<String, Object> pipeline : pipelines) {
      pipelineIdToClass.put(
          (String) pipeline.get("pipelineId"), (String) pipeline.get("classPath"));
    }
    assertEquals(Main1Job.class.getName(), pipelineIdToClass.get(pipelineId1.asEncodedString()));
    assertEquals(Main2Job.class.getName(), pipelineIdToClass.get(pipelineId2.asEncodedString()));
    assertEquals(Main2Job.class.getName(), pipelineIdToClass.get(pipelineId3.asEncodedString()));
  }
}
