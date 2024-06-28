package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.pipeline.*;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Module(
  includes = {
    StepExecutionModule.Bindings.class,
    AppEngineBackendModule.class,
  }
)
public class StepExecutionModule {


  private final PipelineBackEnd backend;

  @Provides @StepExecutionScoped
  public PipelineBackEnd pipelineBackEnd() {
    return backend;
  }


  @Module
  interface Bindings {

    @Binds
    PipelineRunner pipelineRunner(PipelineManager pipelineManager);

    @Binds
    PipelineOrchestrator pipelineOrchestrator(PipelineManager pipelineManager);

    @Binds
    PipelineService pipelineService(PipelineServiceImpl pipelineService);

  }

}
