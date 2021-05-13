package com.google.appengine.tools.pipeline;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import lombok.extern.java.Log;
import org.junit.jupiter.api.extension.*;

/**
 * Junit5 extension to initialize local datastore emulator for tests
 * Use it in your tests with {@code @ExtendWith(DatastoreExtension.class)}
 *
 * TODO: replace with setup for all the pipelines stuff??
 */
@Log
public class DatastoreExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {

  public static String TEST_DATASTORE_PROJECT_ID = "test-project";
  public static String DS_CONTEXT_KEY = "ds-emulator";
  private LocalDatastoreHelper globalDatastoreHelper;

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    globalDatastoreHelper = LocalDatastoreHelper.create(1.0);
    globalDatastoreHelper.start();
    log.info("Datastore emulator started on port : " + globalDatastoreHelper.getPort());
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    globalDatastoreHelper.stop();
    log.info("Datastore emulator stopped");
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    globalDatastoreHelper.reset();
    log.info("Datastore emulator reset");
    Datastore datastore = globalDatastoreHelper.getOptions().toBuilder()
      .setProjectId(TEST_DATASTORE_PROJECT_ID)
      .build().getService();
    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(DS_CONTEXT_KEY, datastore);
  }

  public static class ParameterResolver implements org.junit.jupiter.api.extension.ParameterResolver {

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext) throws ParameterResolutionException {
      return parameterContext.getParameter().getType() == Datastore.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext) throws ParameterResolutionException {
      return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(DatastoreExtension.DS_CONTEXT_KEY);
    }
  }

}

