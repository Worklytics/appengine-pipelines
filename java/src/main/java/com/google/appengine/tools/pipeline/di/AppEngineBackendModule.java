package com.google.appengine.tools.pipeline.di;


import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.appengine.v1.ServicesClient;
import com.google.appengine.v1.VersionsClient;
import com.google.cloud.datastore.Datastore;
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
  public Datastore datastore(AppEngineBackEnd.Options options) {
    return options.getDatastoreOptions().getService();
  }

  @SneakyThrows
  @Provides @StepExecutionScoped
  ServicesClient servicesClient() {
    return ServicesClient.create();
  }

  @SneakyThrows
  @Provides @StepExecutionScoped
  VersionsClient versionsClient() {
    return VersionsClient.create();
  }

  @Provides
  @StepExecutionScoped
  @SneakyThrows
  AppEngineBackEnd.Options appEngineBackEndOptions(PipelineBackEnd pipelineBackEnd) {
    return pipelineBackEnd.getOptions().as(AppEngineBackEnd.Options.class);
  }

  @Provides @StepExecutionScoped
  AppEngineEnvironment appEngineEnvironment() {
    return new AppEngineStandardGen2();
  }

  @Provides @StepExecutionScoped
  AppEngineTaskQueue appEngineTaskQueue() {
    return new AppEngineTaskQueue();
  }


  @Provides @StepExecutionScoped
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
    PipelineBackEnd.Options backendOptions(AppEngineBackEnd.Options options);
  }
}
