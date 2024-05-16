package com.google.appengine.tools.pipeline;


import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Module;
import dagger.Provides;
import lombok.SneakyThrows;

import javax.inject.Singleton;

//TODO: split internals v stuff that can be re-used by others
@Module(
  injects = { //alphabetical order
    PipelineServlet.class,
  },
  library = true,
  complete = false
)
public class DefaultDIModule {

  @Provides @Singleton
  @SneakyThrows
  AppEngineBackEnd.Options appEngineBackEndOptions() {
    return AppEngineBackEnd.Options.builder()
      .projectId(SystemProperty.applicationId.get())
      .credentials(GoogleCredentials.getApplicationDefault())
      .datastoreOptions(DatastoreOptions.getDefaultInstance())
      .build();
  }

  @Provides @Singleton
  PipelineBackEnd.Options backendOptions(AppEngineBackEnd.Options options) {
    return options;
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
