package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;

public class PipelineRunnerFactory {

  public PipelineRunner createPipelineRunner(PipelineBackEnd.Options backendOptions) {
    AppEngineBackEnd backend = new AppEngineBackEnd((AppEngineBackEnd.Options) backendOptions);
    return new PipelineManager(backend);
  }
}
