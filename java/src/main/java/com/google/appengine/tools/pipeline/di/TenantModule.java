package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.cloud.datastore.Datastore;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
@Module(
  includes = {
    TenantModule.Bindings.class,
    TenantModule.AppEngineBackendModule.class,
  }
)
public class TenantModule {

  private final PipelineBackEnd backend;

  @Provides
  public PipelineBackEnd pipelineBackEnd() {
    return backend;
  }


  @Module
  interface Bindings {

    @Binds
    PipelineService pipelineService(PipelineServiceImpl pipelineService);

  }

  @Module(
    includes = TenantModule.AppEngineBackendModule.Bindings.class
  )
  public static class AppEngineBackendModule {

    @Provides @TenantScoped
    public Datastore datastore(AppEngineBackEnd.Options options) {
      return options.getDatastoreOptions().getService();
    }

    @Provides
    @TenantScoped
    @SneakyThrows
    AppEngineBackEnd.Options appEngineBackEndOptions(PipelineBackEnd pipelineBackEnd) {
      return pipelineBackEnd.getOptions().as(AppEngineBackEnd.Options.class);
    }

    @Provides @TenantScoped
    AppEngineEnvironment appEngineEnvironment() {
      return new AppEngineStandardGen2();
    }

    @Provides @TenantScoped
    AppEngineTaskQueue appEngineTaskQueue() {
      return new AppEngineTaskQueue();
    }

    @Provides @TenantScoped
    AppEngineBackEnd appEngineBackEnd(AppEngineBackEnd.Options options,
                                      AppEngineTaskQueue appEngineTaskQueue) {
      return new AppEngineBackEnd(options.getDatastoreOptions().getService(), appEngineTaskQueue);
    }

    @Module
    interface Bindings {

      @Binds
      PipelineBackEnd.Options backendOptions(AppEngineBackEnd.Options options);
    }
  }
}


