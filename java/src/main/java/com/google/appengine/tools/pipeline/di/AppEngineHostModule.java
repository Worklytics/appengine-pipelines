package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.appengine.v1.ServicesClient;
import com.google.appengine.v1.VersionsClient;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.tasks.v2.CloudTasksClient;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import lombok.SneakyThrows;

/**
 * provides general dependencies for AppEngine environments, which aren't coupled to specific tenant
 */
@Module(
  includes = {
    AppEngineHostModule.Bindings.class,
  }
)
public class AppEngineHostModule {

  @SneakyThrows
  @Provides
  ServicesClient servicesClient() {
    return ServicesClient.create();
  }

  @SneakyThrows
  @Provides
  VersionsClient versionsClient() {
    return VersionsClient.create();
  }

  @SneakyThrows
  @Provides
  CloudTasksClient cloudTasksClient() {
    return CloudTasksClient.create();
  }

  @Provides
  AppEngineEnvironment appEngineEnvironment() {
    return new AppEngineStandardGen2();
  }



  @Provides
  AppEngineServicesService appEngineServicesService(AppEngineServicesServiceImpl impl) {

    // TODO: should probably inject AppEngineEnvironment, and get it from there
    DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();

    //before, test harness basically did this by overriding env vars via ApiProxy stuff; see LocalModulesServiceTestConfig
    if (RequestUtils.LOCAL_GAE_PROJECT_ID.equals(defaultInstance.getProjectId()) || "test-project".equals(defaultInstance.getProjectId())) {
      return new AppEngineServicesService() {
        @Override
        public String getDefaultService() {
          return "default";
        }

        @Override
        public String getDefaultVersion(String service) {
          return "1";
        }

        @Override
        public String getWorkerServiceHostName(String service, String version) {
          return String.join(".", service, version, "localhost");
        }
      };
    } else {
      return impl;
    }
  }

  @Module
  interface Bindings {


    @Binds
    PipelineTaskQueue pipelineTaskQueue(CloudTasksTaskQueue appEngineTaskQueue);
  }

}
