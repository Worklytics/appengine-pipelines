package com.google.appengine.tools.pipeline.testutil;

import com.google.appengine.tools.mapreduce.impl.util.RequestUtils;
import com.google.appengine.tools.pipeline.impl.backend.*;
import com.google.appengine.v1.ApplicationsClient;
import com.google.appengine.v1.ServicesClient;
import com.google.appengine.v1.VersionsClient;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.tasks.v2.CloudTasksClient;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import lombok.SneakyThrows;

/**
 * test equivalent of {@link com.google.appengine.tools.pipeline.di.AppEngineHostModule}
 */
@Module(
  includes = AppEngineHostTestModule.Bindings.class
)
public class AppEngineHostTestModule {

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

  @SneakyThrows
  @Provides
  ApplicationsClient applicationsClient() {
    return ApplicationsClient.create();
  }

  @Provides
  AppEngineEnvironment appEngineEnvironment() {
    return new AppEngineStandardGen2();
  }

  @Provides
  AppEngineServicesService appEngineServicesService(AppEngineServicesServiceImpl impl) {

    // TODO: should probably inject AppEngineEnvironment, and get it from there


    //before, test harness basically did this by overriding env vars via ApiProxy stuff; see LocalModulesServiceTestConfig
    if (isTestingContext()) {
      return new AppEngineServicesService() {
        @Override
        public String getLocation() {
          return "us-central1";
        }

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

  boolean isTestingContext() {
    DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();
    return RequestUtils.LOCAL_GAE_PROJECT_ID.equals(defaultInstance.getProjectId()) || "test-project" .equals(defaultInstance.getProjectId());
  }

  @Module
  interface Bindings {
    // this is the real difference for tests atm
    @Binds
    PipelineTaskQueue pipelineTaskQueue(AppEngineTaskQueue taskQueue);
  }
}
