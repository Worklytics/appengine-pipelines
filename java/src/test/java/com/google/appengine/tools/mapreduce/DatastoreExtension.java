package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.EnvironmentUtils;
import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOpenTelemetryOptions;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import lombok.extern.java.Log;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;

import java.net.ConnectException;
import java.util.logging.Level;

/**
 * Junit5 extension to initialize local datastore emulator for tests
 * Use it in your tests with {@code @ExtendWith(DatastoreExtension.class)}
 *
 * TODO: replace with setup for all the pipelines stuff??
 */
@Log
public class DatastoreExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {

  public static String TEST_DATASTORE_PROJECT_ID = EnvironmentUtils.TEST_PROJECT_ID;
  public static String DS_CONTEXT_KEY = "ds-emulator";
  public static String DS_OPTIONS_CONTEXT_KEY = "ds-options";

  private LocalDatastoreHelper globalDatastoreHelper;

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    globalDatastoreHelper = LocalDatastoreHelper.newBuilder()
      .setStoreOnDisk(false)  // can't reset if storing data disk
      .setConsistency(1.0)
      .build();
    globalDatastoreHelper.start();
    log.info("Datastore emulator started on port : " + globalDatastoreHelper.getPort());
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {

    int attempt = 0;
    boolean stopped = false;
    while (!stopped && attempt < 3) {
      ++attempt;
      try {
        org.threeten.bp.Duration timeout =
          org.threeten.bp.Duration.ofSeconds(5).multipliedBy(attempt);
        globalDatastoreHelper.stop(timeout);
        stopped = true;
      } catch (ConnectException e) {
        // don't want to kill the test, but also don't want to leave the emulator running
        log.warning("DatastoreExtension : Failed to connect in order to stop datastore emulator; retrying...");
      } catch (Exception e) {
        log.log(Level.WARNING, "DatastoreExtension : Failed to stop datastore emulator; retrying...", e);
      }
    }
    log.info("Datastore emulator stopped");
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    globalDatastoreHelper.reset();
    log.info("Datastore emulator reset");
    DatastoreOptions.Builder builder = EnvironmentUtils.datastoreBuilderFromDatastoreOptions(globalDatastoreHelper.getOptions());
    builder.setProjectId(TEST_DATASTORE_PROJECT_ID);
    builder.setCredentials(NoCredentials.getInstance());
    builder.setHost("localhost:" + globalDatastoreHelper.getPort());
    builder.setOpenTelemetryOptions(DatastoreOpenTelemetryOptions.newBuilder().build());

    DatastoreOptions options = builder.build();
     extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(DS_OPTIONS_CONTEXT_KEY, options);

    Datastore datastore = options.getService();
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(DS_CONTEXT_KEY, datastore);
  }

  public static class ParameterResolver implements org.junit.jupiter.api.extension.ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext) throws ParameterResolutionException {
      return parameterContext.getParameter().getType() == Datastore.class
        || parameterContext.getParameter().getType() == DatastoreOptions.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext) throws ParameterResolutionException {
      if (parameterContext.getParameter().getType() == Datastore.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_CONTEXT_KEY);
      } else if (parameterContext.getParameter().getType() == DatastoreOptions.class) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_OPTIONS_CONTEXT_KEY);
      } else {
        throw new ParameterResolutionException("Unsupported parameter type: " + parameterContext.getParameter().getType());
      }
    }
  }

}

