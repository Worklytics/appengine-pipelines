package com.google.appengine.tools.pipeline;

import org.junit.jupiter.api.Test;

public class ClientUseTest {


  @Test
  public void testClientUse() {
    //shouldn't die
    PipelineService pipelineService = PipelineService.get();
  }

}
