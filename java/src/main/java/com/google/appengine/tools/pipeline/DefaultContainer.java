package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import dagger.Component;

import javax.inject.Singleton;

@Singleton
@Component(
  modules = {
    DefaultDIModule.class,
  }
)
public interface DefaultContainer extends PipelineContainer {

  void injects(PipelineManager pipelineManager);

}
