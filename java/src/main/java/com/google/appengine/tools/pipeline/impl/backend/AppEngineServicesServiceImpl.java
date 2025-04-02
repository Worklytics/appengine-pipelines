package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.v1.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

/**
 *
 * NOTE: uses guava caches, which should be thread-safe
 * also caches the location of the app, with logic to make this thread-safe
 *
 * Ideally, this is a singleton, but actually marking it as such messes up the Dagger compile for some reason ...
 *
 * The module that binds this IS a singleton, so hopefully that makes this a singleton too in practice.
 */
@Singleton
@Log
public class AppEngineServicesServiceImpl implements AppEngineServicesService {

  enum ConfigProperty implements com.google.appengine.tools.pipeline.util.ConfigProperty {

    /**
     * If set, we assume that all services follow the convention:
     *   {version}-dot-{service}-dot-{project}.{GAE_SERVICE_HOST_SUFFIX}
     *
     * Usage of this is to save API calls to get service hostnames, as in practice are deterministic.
     */
    GAE_SERVICE_HOST_SUFFIX,


    /**
     * If set, we use this service to run the pipeline tasks/workers by default.
     */
    DEFAULT_PIPELINES_SERVICE,
    ;
  }


  private final AppEngineEnvironment appEngineEnvironment;

  private final Provider<ServicesClient>  servicesClientProvider;
  private final Provider<VersionsClient>  versionsClientProvider;
  private final Provider<ApplicationsClient> applicationsClientProvider;

  private static final int MAX_API_CALL_ATTEMPTS = 3;

  @VisibleForTesting
  public static int instanceCount = 0;

  @Inject
  AppEngineServicesServiceImpl(
                               AppEngineEnvironment appEngineEnvironment,
                               Provider<ServicesClient> servicesClientProvider,
                               Provider<VersionsClient> versionsClientProvider,
                               Provider<ApplicationsClient> applicationsClientProvider) {
    this.appEngineEnvironment = appEngineEnvironment;
    this.servicesClientProvider = servicesClientProvider;
    this.versionsClientProvider = versionsClientProvider;
    this.applicationsClientProvider = applicationsClientProvider;

    instanceCount++;

//    // disable for now; this very much IS happening in the main application, which uses this in request-scoped way (which is safe, but not efficient)
////    if (instanceCount > 1) {
////      log.warning("Multiple instances of AppEngineServicesServiceImpl created, this is not expected");
////    }
  }

  @SneakyThrows
  public static AppEngineServicesServiceImpl defaults() {
    return new AppEngineServicesServiceImpl(new AppEngineStandardGen2(), AppEngineServicesServiceImpl::getServicesClientProvider, AppEngineServicesServiceImpl::getVersionsClientProvider, AppEngineServicesServiceImpl::getApplicationsClientProvider);
  }

  static ApplicationsClient getApplicationsClientProvider() {
    try {
      return ApplicationsClient.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static ServicesClient getServicesClientProvider() {
    try {
      return ServicesClient.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static VersionsClient getVersionsClientProvider() {
    try {
      return VersionsClient.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  Cache<String, String> defaultVersionCache = CacheBuilder.newBuilder()
          .maximumSize(10)
          .build();

  Cache<String, String> hostnameCache = CacheBuilder.newBuilder()
          .maximumSize(10)
          .build();

  // would only change on re-deployment
  volatile String location;


  @Override
  public String getDefaultService() {
    return ConfigProperty.DEFAULT_PIPELINES_SERVICE.getValue()
      .orElseGet(appEngineEnvironment::getService);
  }

  @Override
  @SneakyThrows
  public String getDefaultVersion(String service) {
    final String nonNullService = Optional.ofNullable(service).orElseGet(this::getDefaultService);
    return defaultVersionCache.get(nonNullService, () -> getDefaultVersionInternal(nonNullService));
  }

  @SneakyThrows
  @Override
  public String getWorkerServiceHostName(@NonNull String service, @NonNull String version) {
    return ConfigProperty.GAE_SERVICE_HOST_SUFFIX.getValue()
            .map(suffix -> String.format("%s-dot-%s-dot-%s.%s", version, service, appEngineEnvironment.getProjectId(), suffix))
            .orElseGet(() -> {
              try {
                return hostnameCache.get(service + ":" + version, () -> getWorkerServiceHostNameInternal(service, version));
              } catch (ExecutionException e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  public String getLocation() {
    if (location == null) {
      synchronized (this) {
        //double-check for thread safety
        if (location == null) {
          fillLocation();
        }
      }
    }
    return location;
  }

  private synchronized void fillLocation() {
    try (ApplicationsClient applicationsClient = applicationsClientProvider.get()) {
      Application application = applicationsClient.getApplication("apps/" + appEngineEnvironment.getProjectId());
      location = application.getLocationId();
    } catch (Throwable e) {
      log.log(Level.SEVERE, "Failed to retrieve application location", e);
      throw e;
    }
  }


  private String getDefaultVersionInternal(@NonNull String service) {
    if (Objects.equals(service, appEngineEnvironment.getService())) {
      return appEngineEnvironment.getVersion();
    }

    int attempts = 0;
    while (true) {
      try (ServicesClient servicesClient = servicesClientProvider.get()) {
        GetServiceRequest request = GetServiceRequest.newBuilder().setName(serviceEntityNameFragment(service)).build();
        Service response = servicesClient.getService(request);
        return response.getSplit().getAllocationsMap().entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed()).findFirst().orElseThrow().getKey();
      } catch (Exception e) {
        if (++attempts >= MAX_API_CALL_ATTEMPTS) {
          throw e;
        } else {
          log.log(Level.WARNING, "Exception seen, retrying", e);
        }
      }
    }
  }

  private  String getWorkerServiceHostNameInternal(String service, String version) {
    int attempts = 0;
    while (true) {
      try (VersionsClient versionsClient = this.versionsClientProvider.get()) {
        Version versionResponse = versionsClient.getVersion(GetVersionRequest.newBuilder().setName(serviceEntityNameFragment(service) + "/versions/" + version).build());
        return versionResponse.getVersionUrl().replace("https://", "");
      } catch (Exception e) {
        if (++attempts >= MAX_API_CALL_ATTEMPTS) {
          throw e;
        } else {
          log.log(Level.WARNING, "Exception seen, retrying", e);
        }
      }
    }
  }

  private String serviceEntityNameFragment(String service) {
    return "apps/" + appEngineEnvironment.getProjectId() + "/services/" + service;
  }

  @VisibleForTesting
  void fillCache(String service, String version) {
    defaultVersionCache.put(service, version);
  }
}


