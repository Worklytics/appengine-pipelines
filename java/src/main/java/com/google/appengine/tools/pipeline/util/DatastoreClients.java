package com.google.appengine.tools.pipeline.util;

import com.google.cloud.TransportOptions;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.extern.java.Log;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Shared Datastore clients, one per distinct {@link DatastoreOptions}.
 *
 * Each client holds a gRPC channel (and its connection) until closed, so clients must not be created per request:
 * the ones never closed are never released (see https://docs.cloud.google.com/java/docs/client-lifecycle). Clients
 * are thread-safe and meant to be long-lived and shared; transactions are not, but each caller creates its own.
 *
 * Clients returned here are shared: don't close them.
 */
@Log
public final class DatastoreClients {

  /**
   * more distinct options than this suggests they don't compare equal when they should (so every lookup creates a
   * client, as before this cache)
   */
  @VisibleForTesting
  static final int EXPECTED_MAX_CLIENTS = 100;

  // DatastoreOptions.equals() ignores transport options, which determine the kind of client built
  private record ClientKey(DatastoreOptions options, TransportOptions transportOptions) { }

  private static final ConcurrentMap<ClientKey, Datastore> CLIENTS = new ConcurrentHashMap<>();

  private DatastoreClients() { }

  /**
   * @return the shared client for options equal to {@code options}, creating it if needed
   */
  public static Datastore get(@NonNull DatastoreOptions options) {
    return CLIENTS.computeIfAbsent(new ClientKey(options, options.getTransportOptions()), key -> {
      int clientsSize = CLIENTS.size();
      if (clientsSize >= EXPECTED_MAX_CLIENTS) {
        log.warning("Created " + (clientsSize + 1) + " Datastore clients; options expected to be equal may not be");
      }
      log.fine(() -> "Creating Datastore client for project " + options.getProjectId() + ", database "
        + options.getDatabaseId() + ", namespace " + options.getNamespace() + ", host " + options.getHost());
      return options.getService();
    });
  }

  @VisibleForTesting
  static int size() {
    return CLIENTS.size();
  }
}
