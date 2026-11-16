package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.pipeline.util.GoogleApiClients;
import com.google.appengine.tools.EnvironmentUtils;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineEnvironment;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesService;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesServiceImpl;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineStandardGen2;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueue;
import com.google.appengine.tools.pipeline.impl.backend.CloudTasksTaskQueue;
import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.v1.ApplicationsClient;
import com.google.appengine.v1.ServicesClient;
import com.google.appengine.v1.VersionsClient;
import com.google.cloud.tasks.v2.CloudTasksClient;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;
import java.util.Optional;

/**
 * provides general dependencies for AppEngine environments, which aren't coupled to specific tenant
 */
@Module
public class AppEngineHostModule {

  // properties to control behavior, via JVM system properties or env vars (fallback)
  enum ConfigProperties {
    USE_LEGACY_QUEUES,
    USE_LOCAL_SERVICE,
    ;

    public Optional<String> get() {
      return Optional.ofNullable(System.getProperty(name(), System.getenv(name())));
    }

    public Optional<Boolean> getBoolean() {
      return get().map(Boolean::parseBoolean);
    }
  }

  // process-wide, not per component; callers must not close it
  @Provides
  ServicesClient servicesClient() {
    return GoogleApiClients.services();
  }

  // process-wide, not per component; callers must not close it
  @Provides
  VersionsClient versionsClient() {
    return GoogleApiClients.versions();
  }

  // process-wide, not per component; callers must not close it
  @Provides
  ApplicationsClient applicationsClient() {
    return GoogleApiClients.applications();
  }

  // process-wide, not per component; callers must not close it
  @Provides
  CloudTasksClient cloudTasksClient() {
    return GoogleApiClients.cloudTasks();
  }

  @Provides @Singleton
  AppEngineEnvironment appEngineEnvironment() {
    return new AppEngineStandardGen2();
  }

  @Provides @Singleton
  AppEngineServicesService appEngineServicesService(AppEngineServicesServiceImpl impl,
                                                    AppEngineEnvironment environment) {
    //before, test harness basically did this by overriding env vars via ApiProxy stuff; see LocalModulesServiceTestConfig
    if (isTestingContext(environment) || ConfigProperties.USE_LOCAL_SERVICE.getBoolean().orElse(false)) {
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

  @Provides @Singleton
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
    return EnvironmentUtils.isNotCloudEnvironment(environment.getProjectId());
  }
}
