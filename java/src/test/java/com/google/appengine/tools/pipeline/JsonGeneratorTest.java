package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.servlets.JsonGenerator;

import static com.google.appengine.tools.pipeline.TestUtils.waitUntilJobComplete;

public class JsonGeneratorTest extends PipelineTest {

  public JsonGeneratorTest() {
    super();
  }

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

  public void testPipelineObjectsToJson() throws Exception {

    PipelineService service = PipelineServiceFactory.newPipelineService();
    ConcreteJob job = new ConcreteJob();
    String pipelineId = service.startNewPipeline(job);
    JobRecord jobRecord = PipelineManager.getJob(pipelineId);
    assertEquals(job.getJobDisplayName(), jobRecord.getRootJobDisplayName());
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
    jobRecord = PipelineManager.getJob(pipelineId);
    assertEquals(job.getJobDisplayName(), jobRecord.getRootJobDisplayName());
    PipelineObjects pobjects = PipelineManager.queryFullPipeline(pipelineId);

    //test (fairly pointless, but ensures no loops/etc at least)
    String json = JsonGenerator.pipelineObjectsToJson(pobjects);
    assertTrue(json.contains(pipelineId));
  }
}
