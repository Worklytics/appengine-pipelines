package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import org.junit.jupiter.api.Test;

public class ClientUseTest {


  @Test
  public void testClientUse() {
    //shouldn't die
    PipelineService pipelineService = PipelineService.getInstance();

    PipelineManager pipelineManager = PipelineManager.getInstance();
  }

}
