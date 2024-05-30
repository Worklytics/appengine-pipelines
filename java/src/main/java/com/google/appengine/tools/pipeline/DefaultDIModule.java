package com.google.appengine.tools.pipeline;


import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.DatastoreOptions;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import lombok.SneakyThrows;

import javax.inject.Singleton;

//TODO: split internals v stuff that can be re-used by others
@Module(
  includes = DefaultDIModule.Bindings.class
)
public class DefaultDIModule {

  @Provides
  @Singleton
  @SneakyThrows
  AppEngineBackEnd.Options appEngineBackEndOptions() {
    return AppEngineBackEnd.Options.builder()
      .projectId(SystemProperty.applicationId.get())
      .credentials(GoogleCredentials.getApplicationDefault())
      .datastoreOptions(DatastoreOptions.getDefaultInstance())
      .build();
  }

  @Provides
  @Singleton
  PipelineBackEnd pipelineBackEnd(AppEngineBackEnd appEngineBackEnd) {
    return appEngineBackEnd;
  }

  @Provides
  @Singleton
  AppEngineEnvironment appEngineEnvironment() {
    return new AppEngineStandardGen2();
  }

  @Provides
  @Singleton
  AppEngineTaskQueue appEngineTaskQueue() {
    return new AppEngineTaskQueue();
  }

  @Provides
  @Singleton
  AppEngineBackEnd appEngineBackEnd(AppEngineBackEnd.Options options,
                                    AppEngineTaskQueue appEngineTaskQueue) {
    return new AppEngineBackEnd(options.getDatastoreOptions().getService(), appEngineTaskQueue);
  }

  @Provides @Singleton
  RequestUtils provideRequestUtils() {
    return new RequestUtils();
  }


  @Module
  interface Bindings {
    @Binds
    PipelineBackEnd.Options backendOptions(AppEngineBackEnd.Options options);

    @Binds
    PipelineRunner pipelineRunner(PipelineManager pipelineManager);

    @Binds
    PipelineOrchestrator pipelineOrchestrator(PipelineManager pipelineManager);

    @Binds
    PipelineService pipelineService(PipelineServiceImpl pipelineService);
  }
}
