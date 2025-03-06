package com.google.appengine.tools.pipeline.di;


import com.google.appengine.tools.pipeline.PipelineOrchestrator;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import lombok.SneakyThrows;


//TODO: split internals v stuff that can be re-used by others
@Module(
  includes = AppEngineBackendModule.Bindings.class
)
public class AppEngineBackendModule {

  @Provides @StepExecutionScoped
  public Datastore datastore(DatastoreOptions options) {
    return options.getService();
  }

  @Provides
  @StepExecutionScoped
  @SneakyThrows
  AppEngineBackEnd.Options appEngineBackEndOptions(PipelineBackEnd pipelineBackEnd) {
    return pipelineBackEnd.getOptions().as(AppEngineBackEnd.Options.class);
  }

  @Provides @StepExecutionScoped
  AppEngineBackEnd appEngineBackEnd(
    Datastore datastore,
                                    AppEngineTaskQueue appEngineTaskQueue,
    AppEngineServicesService appEngineServicesService
                                    ) {
    return new AppEngineBackEnd(datastore, appEngineTaskQueue, appEngineServicesService);
  }

  @Module
  interface Bindings {

    @Binds @StepExecutionScoped
    PipelineBackEnd pipelineBackEnd(AppEngineBackEnd appEngineBackEnd);

  }
}
