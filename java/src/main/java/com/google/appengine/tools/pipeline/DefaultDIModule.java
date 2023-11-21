package com.google.appengine.tools.pipeline;


import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Module;
import dagger.Provides;
import lombok.SneakyThrows;

import javax.inject.Singleton;

@Module(
  injects = { //alphabetical order
    Job.class,
  },
  library = true,
  complete = false
)
public class DefaultDIModule {

  @Provides @Singleton
  @SneakyThrows
  PipelineBackEnd.Options backendOptions() {
    return AppEngineBackEnd.Options.builder()
      .projectId(SystemProperty.applicationId.get())
      .credentials(GoogleCredentials.getApplicationDefault())
      .datastoreOptions(DatastoreOptions.getDefaultInstance())
      .build();
  }


  @Provides @Singleton
  AppEngineEnvironment appEngineEnvironment() {
    return new AppEngineStandardGen2();
  }

  @Provides @Singleton
  AppEngineTaskQueue appEngineTaskQueue() {
    return new AppEngineTaskQueue();
  }
  @Provides @Singleton
  AppEngineBackEnd appEngineBackEnd(AppEngineBackEnd.Options options,
                                    AppEngineTaskQueue appEngineTaskQueue) {
    return new AppEngineBackEnd(options.getDatastoreOptions().getService(), appEngineTaskQueue);
  }

  @Provides @Singleton
  PipelineManager pipelineManager(AppEngineBackEnd backend) {
    return new PipelineManager(backend);
  }

  @Provides @Singleton
  PipelineRunner pipelineRunner(PipelineManager pipelineManager) {
    return pipelineManager;
  }

}
