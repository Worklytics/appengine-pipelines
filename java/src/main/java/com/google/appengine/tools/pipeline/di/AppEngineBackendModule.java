package com.google.appengine.tools.pipeline.di;

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
  AppEngineBackEnd.Options appEngineBackEndOptions(AppEngineBackEnd pipelineBackEnd) {
    return pipelineBackEnd.getOptions().as(AppEngineBackEnd.Options.class);
  }

  @Module
  interface Bindings {

    @Binds
    PipelineBackEnd appEngineBackEnd(AppEngineBackEnd appEngineBackEnd);
  }
}
