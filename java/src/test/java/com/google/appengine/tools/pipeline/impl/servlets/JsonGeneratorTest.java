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
  static final String EXAMPLE_JSON_RESPONSE = "{\"rootPipelineId\":\"0\",\"slots\":{\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=18}]}\":{\"fillTimeMs\":1715806018140,\"status\":\"filled\"},\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=13}]}\":{\"fillTimeMs\":1715806018267,\"fillerPipelineId\":\"partition_id+%7B%0A++project_id%3A+%22test-project%22%0A%7D%0Apath+%7B%0A++kind%3A+%22pipeline-job%22%0A++name%3A+%229%22%0A%7D%0A\",\"status\":\"filled\"},\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=14}]}\":{\"fillTimeMs\":1715806017978,\"status\":\"filled\"},\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=4}]}\":{\"fillTimeMs\":1715806018340,\"fillerPipelineId\":\"partition_id+%7B%0A++project_id%3A+%22test-project%22%0A%7D%0Apath+%7B%0A++kind%3A+%22pipeline-job%22%0A++name%3A+%229%22%0A%7D%0A\",\"status\":\"filled\"},\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=5}]}\":{\"fillTimeMs\":1715806011209,\"status\":\"filled\"}},\"pipelines\":{\"0\":{\"outputs\":{\"default\":\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=4}]}\"},\"backoffFactor\":2,\"backoffSeconds\":15,\"startTimeMs\":1715806017931,\"currentAttempt\":1,\"endTimeMs\":1715806018340,\"args\":[],\"maxAttempts\":3,\"queueName\":\"default\",\"children\":[\"partition_id+%7B%0A++project_id%3A+%22test-project%22%0A%7D%0Apath+%7B%0A++kind%3A+%22pipeline-job%22%0A++name%3A+%229%22%0A%7D%0A\"],\"classPath\":\"ConcreteJob\",\"kwargs\":{},\"afterSlotKeys\":[\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=5}]}\"],\"status\":\"done\"},\"9\":{\"outputs\":{\"default\":\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=13}]}\"},\"backoffFactor\":2,\"backoffSeconds\":15,\"startTimeMs\":1715806018126,\"currentAttempt\":1,\"endTimeMs\":1715806018267,\"args\":[],\"maxAttempts\":3,\"queueName\":\"default\",\"children\":[],\"classPath\":\"ConcreteJob\",\"kwargs\":{},\"afterSlotKeys\":[\"Key{projectId=test-project,namespace=,databaseId=,path=[PathElement{kind=pipeline-slot,id=null,name=14}]}\"],\"status\":\"done\"}}}";
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
