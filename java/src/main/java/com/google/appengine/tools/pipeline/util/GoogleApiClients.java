package com.google.appengine.tools.pipeline.util;

import com.google.appengine.v1.ApplicationsClient;
import com.google.appengine.v1.ServicesClient;
import com.google.appengine.v1.VersionsClient;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.io.IOException;

/**
 * Process-wide Cloud Tasks and App Engine Admin API clients, each created once, on first use.
 *
 * These are gRPC clients: thread-safe, and meant to be long-lived and shared (see
 * https://docs.cloud.google.com/java/docs/client-lifecycle). Shared per process rather than per Dagger component,
 * as components may be built more than once; a client per component would never be closed.
 *
 * Callers must not close them.
 */
public final class GoogleApiClients {

  private GoogleApiClients() { }

  private interface ClientFactory<T> {
    T create() throws IOException;
  }

  private static <T> Supplier<T> shared(ClientFactory<T> factory) {
    return Suppliers.memoize(() -> {
      try {
        return factory.create();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static final Supplier<CloudTasksClient> CLOUD_TASKS = shared(CloudTasksClient::create);
  private static final Supplier<ServicesClient> SERVICES = shared(ServicesClient::create);
  private static final Supplier<VersionsClient> VERSIONS = shared(VersionsClient::create);
  private static final Supplier<ApplicationsClient> APPLICATIONS = shared(ApplicationsClient::create);

  public static CloudTasksClient cloudTasks() {
    return CLOUD_TASKS.get();
  }

  public static ServicesClient services() {
    return SERVICES.get();
  }

  public static VersionsClient versions() {
    return VERSIONS.get();
  }

  public static ApplicationsClient applications() {
    return APPLICATIONS.get();
  }
}
