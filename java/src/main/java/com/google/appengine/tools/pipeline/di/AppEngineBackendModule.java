package com.google.appengine.tools.pipeline.di;


import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CloudTasksSettings;
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


  @SneakyThrows
  @Provides  @StepExecutionScoped
  CloudTasksClient cloudTasksClient(AppEngineBackEnd.Options options) {
    return CloudTasksClient.create(CloudTasksSettings.newBuilder()
      .setCredentialsProvider(() -> options.getCredentials())
      .build());
  }

  @Provides @StepExecutionScoped
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
