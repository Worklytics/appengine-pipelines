package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.appengine.tools.pipeline.TestUtils.waitUntilJobComplete;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JsonGeneratorTest extends PipelineTest {

  //example response
  static final String EXAMPLE_JSON_RESPONSE = "{\"rootPipelineId\":\"0\",\"slots\":{\"agR0ZXN0chULEg1waXBlbGluZS1zbG90IgIxNAw\":{\"fillTimeMs\":1573682430427,\"status\":\"filled\"},\"agR0ZXN0chULEg1waXBlbGluZS1zbG90IgIxMww\":{\"fillTimeMs\":1573682430576,\"fillerPipelineId\":\"9\",\"status\":\"filled\"},\"agR0ZXN0chQLEg1waXBlbGluZS1zbG90IgE0DA\":{\"fillTimeMs\":1573682430623,\"fillerPipelineId\":\"9\",\"status\":\"filled\"},\"agR0ZXN0chULEg1waXBlbGluZS1zbG90IgIxOAw\":{\"fillTimeMs\":1573682430504,\"status\":\"filled\"},\"agR0ZXN0chQLEg1waXBlbGluZS1zbG90IgE1DA\":{\"fillTimeMs\":1573682430155,\"status\":\"filled\"}},\"pipelines\":{\"0\":{\"outputs\":{\"default\":\"agR0ZXN0chQLEg1waXBlbGluZS1zbG90IgE0DA\"},\"backoffFactor\":2,\"backoffSeconds\":15,\"startTimeMs\":1573682430424,\"currentAttempt\":1,\"endTimeMs\":1573682430623,\"args\":[],\"maxAttempts\":3,\"queueName\":\"default\",\"children\":[\"9\"],\"classPath\":\"ConcreteJob\",\"kwargs\":{},\"afterSlotKeys\":[\"agR0ZXN0chQLEg1waXBlbGluZS1zbG90IgE1DA\"],\"status\":\"done\"},\"9\":{\"outputs\":{\"default\":\"agR0ZXN0chULEg1waXBlbGluZS1zbG90IgIxMww\"},\"backoffFactor\":2,\"backoffSeconds\":15,\"startTimeMs\":1573682430500,\"currentAttempt\":1,\"args\":[],\"maxAttempts\":3,\"queueName\":\"default\",\"children\":[],\"classPath\":\"ConcreteJob\",\"kwargs\":{},\"afterSlotKeys\":[\"agR0ZXN0chULEg1waXBlbGluZS1zbG90IgIxNAw\"],\"status\":\"run\"}}}";

  private static class ConcreteJob extends Job0 {

    @Override
    public String getJobDisplayName() {
      return "ConcreteJob";
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(Throwable t) {
      return immediate("Got exception!");
    }

    @Override
    public Value run() throws Exception {
      return immediate(null);
    }
  }


  //TODO: this stuff should all be mocked, so can control jobIds + timestamps, more properly validate JSON
  // outputs
  PipelineObjects exampleObjects() throws Exception {


    ConcreteJob job = new ConcreteJob();

    //job ids seem to auto-inc in stubbed local data store, so beware ...
    String pipelineId = pipelineService.startNewPipeline(job);
    JobRecord jobRecord = pipelineManager.getJob(pipelineId);
    JobInfo jobInfo = waitUntilJobComplete(pipelineService, pipelineId);
    jobRecord = pipelineManager.getJob(pipelineId);
    return pipelineManager.queryFullPipeline(pipelineId);
  }

  @Test
  public void testMap() throws Exception {
    Map<String, Object> asMap = JsonGenerator.objectsToMapRepresentation(exampleObjects());

    assertEquals(3, asMap.size());
    assertNotNull(asMap.get("rootPipelineId"));
    assertNotNull(asMap.get("slots"));
    assertNotNull(asMap.get("pipelines"));
  }

  @Test
  public void testPipelineObjectsToJson() throws Exception {

    //test (fairly pointless, but ensures no loops/etc at least)
    PipelineObjects example = exampleObjects();
    String json = stripWhitespace(JsonGenerator.pipelineObjectsToJson(example));

    int length = EXAMPLE_JSON_RESPONSE.length();
    //assertEquals(length, json.length());
    assertEquals(EXAMPLE_JSON_RESPONSE.substring(25, 50), json.substring(25, 50));
    assertEquals(EXAMPLE_JSON_RESPONSE.substring(length - 100, length), json.substring(length - 100, length));
  }

  private String stripWhitespace(String s) {
    return s.replaceAll("\\s+","");
  }
}
