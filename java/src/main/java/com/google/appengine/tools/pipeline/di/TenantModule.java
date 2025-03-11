package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.cloud.datastore.Datastore;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
@Module(
  includes = {
    TenantModule.AppEngineBackendModule.class,
    AppEngineHostModule.class
  }
)
public class TenantModule {

  private final PipelineBackEnd backend;

  @Provides
  public PipelineBackEnd pipelineBackEnd() {
    return backend;
  }

  @Module(
    includes = AppEngineBackendModule.Bindings.class
  )
  public static class AppEngineBackendModule {

    @Provides
    public Datastore datastore(AppEngineBackEnd.Options options) {
      return options.getDatastoreOptions().getService();
    }

    @Provides
    @SneakyThrows
    AppEngineBackEnd.Options appEngineBackEndOptions(PipelineBackEnd pipelineBackEnd) {
      return pipelineBackEnd.getOptions().as(AppEngineBackEnd.Options.class);
    }

    @Provides
    AppEngineBackEnd appEngineBackEnd(
      AppEngineBackEnd.Options options,
      AppEngineTaskQueue appEngineTaskQueue,
      AppEngineServicesService appEngineServicesService
    ) {
      return new AppEngineBackEnd(options.getDatastoreOptions().getService(), appEngineTaskQueue, appEngineServicesService);
    }

    @Module
    interface Bindings {

      @Binds
      PipelineService pipelineService(PipelineServiceImpl pipelineService);

    }
  }
}


