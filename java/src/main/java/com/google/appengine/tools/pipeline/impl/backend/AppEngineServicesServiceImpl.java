package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.v1.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ServiceException;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;


@Log
public class AppEngineServicesServiceImpl implements AppEngineServicesService {

  private final AppEngineEnvironment appEngineEnvironment;

  private final Provider<ServicesClient>  servicesClientProvider;
  private final Provider<VersionsClient>  versionsClientProvider;

  private static final int MAX_API_CALL_ATTEMPTS = 3;

  @Inject
  AppEngineServicesServiceImpl(AppEngineEnvironment appEngineEnvironment,
                               Provider<ServicesClient> servicesClientProvider,
                               Provider<VersionsClient> versionsClientProvider) {
    this.appEngineEnvironment = appEngineEnvironment;
    this.servicesClientProvider = servicesClientProvider;
    this.versionsClientProvider = versionsClientProvider;
  }

  @SneakyThrows
  public static AppEngineServicesServiceImpl defaults() {
    return new AppEngineServicesServiceImpl(new AppEngineStandardGen2(), AppEngineServicesServiceImpl::getServicesClientProvider, AppEngineServicesServiceImpl::getVersionsClientProvider);
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

  @Override
  public String getDefaultService() {
    return appEngineEnvironment.getService();
  }

  @Override
  @SneakyThrows
  public String getDefaultVersion(String service) {

    final String key = Optional.ofNullable(service).orElse(appEngineEnvironment.getService());

    return defaultVersionCache.get(key, () -> getDefaultVersionInternal(key));
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

  @Override
  public String getWorkerServiceHostName(String service, String version) {
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


