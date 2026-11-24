package com.google.appengine.tools.pipeline.util;

import com.google.appengine.tools.EnvironmentUtils;
import com.google.appengine.tools.mapreduce.DatastoreExtension;
import com.google.appengine.tools.pipeline.di.AppEngineBackendModule;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({DatastoreExtension.class, DatastoreExtension.ParameterResolver.class})
public class DatastoreClientsTest {

  private static final String KIND = "DatastoreClientsTest";

  @Test
  public void equalOptionsShareOneClient(DatastoreOptions emulator) {
    Datastore first = DatastoreClients.get(sameAs(emulator));
    int clients = DatastoreClients.size();

    assertSame(first, DatastoreClients.get(sameAs(emulator)));
    assertEquals(clients, DatastoreClients.size(), "equal options created another client");
  }

  @Test
  public void differentOptionsGetDifferentClients(DatastoreOptions emulator) {
    DatastoreOptions otherNamespace = sameAs(emulator).toBuilder().setNamespace("other").build();

    assertNotSame(DatastoreClients.get(sameAs(emulator)), DatastoreClients.get(otherNamespace));
  }

  /**
   * each request builds its own (equal) options; it used to get a new client from them, never closed, so each
   * request leaked the client's gRPC channel and connection
   */
  @Test
  public void requestScopedProviderReusesClient(DatastoreOptions emulator) {
    AppEngineBackendModule module = new AppEngineBackendModule();

    assertSame(module.datastore(sameAs(emulator)), module.datastore(sameAs(emulator)));
  }

  /**
   * concurrent requests share a client; each runs its own transactions on it
   */
  @Test
  public void concurrentRequestsShareClientWithOwnTransactions(DatastoreOptions emulator) throws Exception {
    final int threads = 16;
    final int incrementsPerThread = 25;
    Datastore expectedClient = DatastoreClients.get(sameAs(emulator));

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> results = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final String counterName = "counter-" + t;
        results.add(executor.submit(() -> {
          start.await();
          for (int i = 0; i < incrementsPerThread; i++) {
            // as a request does: its own options, the shared client, its own transaction
            Datastore datastore = DatastoreClients.get(sameAs(emulator));
            assertSame(expectedClient, datastore);
            increment(datastore, counterName);
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> result : results) {
        result.get(2, TimeUnit.MINUTES); // rethrows any failure in the thread
      }
    } finally {
      executor.shutdownNow();
    }

    for (int t = 0; t < threads; t++) {
      Entity counter = expectedClient.get(counterKey(expectedClient, "counter-" + t));
      assertNotNull(counter);
      assertEquals(incrementsPerThread, counter.getLong("value"), "lost or duplicated writes on counter-" + t);
    }
  }

  private static void increment(Datastore datastore, String counterName) {
    Transaction tx = datastore.newTransaction();
    try {
      Key key = counterKey(datastore, counterName);
      Entity current = tx.get(key);
      long value = current == null ? 0 : current.getLong("value");
      tx.put(Entity.newBuilder(key).set("value", value + 1).build());
      tx.commit();
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  private static Key counterKey(Datastore datastore, String name) {
    return datastore.newKeyFactory().setKind(KIND).newKey(name);
  }

  /**
   * new options equal to {@code options}, built as each request builds its own
   */
  private static DatastoreOptions sameAs(DatastoreOptions options) {
    return EnvironmentUtils.datastoreBuilderFromDatastoreOptions(options).build();
  }
}
