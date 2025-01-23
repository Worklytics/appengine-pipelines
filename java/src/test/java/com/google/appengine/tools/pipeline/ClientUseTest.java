package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.test.PipelineSetupExtensions;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import org.junit.jupiter.api.Test;

@PipelineSetupExtensions
public class ClientUseTest {


  @Test
  public void testClientUse(AppEngineBackEnd appEngineBackEnd) {
    //shouldn't die
    PipelineService pipelineService = PipelineService.getInstance((AppEngineBackEnd.Options) appEngineBackEnd.getOptions());

    PipelineManager pipelineManager = PipelineManager.getInstance((AppEngineBackEnd.Options) appEngineBackEnd.getOptions());
  }

}
