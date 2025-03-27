package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.pipeline.PipelineOrchestrator;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import dagger.Binds;
import dagger.Module;

@Module
public interface PipelinesBindings {

  @Binds @StepExecutionScoped
  PipelineService pipelineService(PipelineServiceImpl impl);

  @Binds @StepExecutionScoped
  PipelineRunner pipelineRunner(PipelineManager impl);

  @Binds @StepExecutionScoped
  PipelineOrchestrator pipelineOrchestrator(PipelineManager impl);
}
