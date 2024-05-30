package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineManager;

public interface PipelineContainer {

  void injects(PipelineManager pipelineManager);


}
