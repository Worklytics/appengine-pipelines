package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.pipeline.util.DatastoreClients;
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
      return DatastoreClients.get(options.getDatastoreOptions());
    }

    @Provides  @TenantScoped
    AppEngineBackEnd appEngineBackEnd(
      AppEngineBackEnd.Options options,
      PipelineTaskQueue pipelineTaskQueue,
      AppEngineServicesService appEngineServicesService
    ) {
      return new AppEngineBackEnd(DatastoreClients.get(options.getDatastoreOptions()), pipelineTaskQueue, appEngineServicesService);
    }

    @Module
    interface Bindings {

      @Binds @TenantScoped
      PipelineService pipelineService(PipelineServiceImpl pipelineService);

      @Binds @TenantScoped
      PipelineBackEnd appEngineBackEnd(AppEngineBackEnd appEngineBackEnd);
    }
  }
}


