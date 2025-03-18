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

  // just pass pipeline options instead?? prob.
  private final AppEngineBackEnd.Options backendOptions;

  @Provides @TenantScoped
  AppEngineBackEnd.Options provideBackendOptions() {
    return backendOptions;
  }

  @Module(
    includes = AppEngineBackendModule.Bindings.class
  )
  public static class AppEngineBackendModule {

    @Provides @TenantScoped
    public Datastore datastore(AppEngineBackEnd.Options options) {
      return options.getDatastoreOptions().getService();
    }

    @Provides  @TenantScoped
    AppEngineBackEnd appEngineBackEnd(
      AppEngineBackEnd.Options options,
      PipelineTaskQueue pipelineTaskQueue,
      AppEngineServicesService appEngineServicesService
    ) {
      return new AppEngineBackEnd(options.getDatastoreOptions().getService(), pipelineTaskQueue, appEngineServicesService);
    }

    @Module
    interface Bindings {

      @Binds
      PipelineService pipelineService(PipelineServiceImpl pipelineService);

      @Binds
      PipelineBackEnd appEngineBackEnd(AppEngineBackEnd appEngineBackEnd);
    }
  }
}


