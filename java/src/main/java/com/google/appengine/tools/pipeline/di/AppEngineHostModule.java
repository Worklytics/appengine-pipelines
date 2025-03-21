package com.google.appengine.tools.pipeline.di;

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

import java.util.Optional;

/**
 * provides general dependencies for AppEngine environments, which aren't coupled to specific tenant
 */
@Module
public class AppEngineHostModule {

  // properties to control behavior, via JVM system properties or env vars (fallback)
  enum ConfigProperties {
    USE_LEGACY_QUEUES,
    ;

    public Optional<String> get() {
      return Optional.ofNullable(System.getProperty(name(), System.getenv(name())));
    }

    public Optional<Boolean> getBoolean() {
      return get().map(Boolean::parseBoolean);
    }
  }

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
  ApplicationsClient applicationsClient() {
    return ApplicationsClient.create();
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
  AppEngineServicesService appEngineServicesService(AppEngineServicesServiceImpl impl,
                                                    AppEngineEnvironment environment) {
    //before, test harness basically did this by overriding env vars via ApiProxy stuff; see LocalModulesServiceTestConfig
    if (isTestingContext(environment)) {
      return new AppEngineServicesService() {
        @Override
        public String getLocation() {
          return "us-central1";
        }

        @Override
        public String getDefaultService() {
          return Optional.ofNullable(environment.getService()).orElse("default");
        }

        @Override
        public String getDefaultVersion(String service) {
          return Optional.ofNullable(environment.getVersion()).orElse("v1");
        }

        @Override
        public String getWorkerServiceHostName(String service, String version) {
          return "localhost";
        }
      };
    } else {
      return impl;
    }
  }

  @Provides
  PipelineTaskQueue pipelineTaskQueue(AppEngineEnvironment environment,
                                      CloudTasksTaskQueue cloudTasksTaskQueue,
                                      AppEngineTaskQueue appEngineTaskQueue) {
    if (isTestingContext(environment) || ConfigProperties.USE_LEGACY_QUEUES.getBoolean().orElse(false)) {
      return appEngineTaskQueue;
    } else {
      return cloudTasksTaskQueue;
    }
  }

  boolean isTestingContext(AppEngineEnvironment environment) {
    return RequestUtils.LOCAL_GAE_PROJECT_ID.equals(environment.getProjectId()) || "test-project" .equals(environment.getProjectId());
  }
}
