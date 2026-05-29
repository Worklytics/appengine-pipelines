package com.google.appengine.tools;

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.DatastoreOpenTelemetryOptions;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.java.Log;

@Log
public class EnvironmentUtils {

  // value of env var GOOGLE_CLOUD_PROJECT when running locally; underscores aren't actually legal in GCP project ids,
  // so if this ever ends up being used in a real GCP API call, it blows up in validation before request is even sent by client
  public static final String LOCAL_GAE_PROJECT_ID = "no_app_id";
  public static final String TEST_PROJECT_ID = "test-project";
  public static final String DEFAULT_OVERRIDE_LOCAL_GAE_PROJECT_ID = "local-gae-project";

  /**
   * Builds a DatastoreOptions.Builder pre-populated from the default instance, using
   * NoCredentials in testing/emulator contexts (no ADC, or local GAE project id).
   * @return
   */
  public static DatastoreOptions.Builder datastoreBuilderFromDefaultInstance() {
    DatastoreOptions defaultInstance = DatastoreOptions.getDefaultInstance();
    return datastoreBuilderFromDatastoreOptions(defaultInstance);
  }

  @VisibleForTesting
  public static DatastoreOptions.Builder datastoreBuilderFromDatastoreOptions(DatastoreOptions datastoreOptions) {
    // in case this needs to be overridden, there is a bug in toBuilder that loses the host
    // so we need to copy over everything
    DatastoreOptions.Builder builder = DatastoreOptions.newBuilder()
      .setProjectId(datastoreOptions.getProjectId())
      .setTransportOptions(datastoreOptions.getTransportOptions())
      .setDatabaseId(datastoreOptions.getDatabaseId())
      .setNamespace(datastoreOptions.getNamespace())
      .setHost(datastoreOptions.getHost())
      .setOpenTelemetryOptions(datastoreOptions.getOpenTelemetryOptions());

    if (isNotCloudEnvironment(datastoreOptions)) {
      // set emulator host if needed
      if (getDatastoreEmulatorHost() != null) {
        builder.setHost(getDatastoreEmulatorHost());
      }
      // override credentials
      builder.setCredentials(NoCredentials.getInstance());
      // set valid project id if needed
      if (LOCAL_GAE_PROJECT_ID.equals(datastoreOptions.getProjectId())) {
        log.info("pipelines fw detected running locally with GAE projectId set as '%s'; this isn't legal GCP project id, so changing to '%s'".formatted(LOCAL_GAE_PROJECT_ID, DEFAULT_OVERRIDE_LOCAL_GAE_PROJECT_ID));
        builder.setProjectId(DEFAULT_OVERRIDE_LOCAL_GAE_PROJECT_ID);
      }
      builder.setOpenTelemetryOptions(DatastoreOpenTelemetryOptions.newBuilder().build());
    } else if (datastoreOptions.getCredentials() != null) {
      builder.setCredentials(datastoreOptions.getCredentials());
    } else {
      log.warning("No credentials found for DatastoreOptions.Builder?");
    }

    return builder;
  }

    @VisibleForTesting
  public static boolean isNotCloudEnvironment() {
    return isNotCloudEnvironment(DatastoreOptions.getDefaultInstance());
  }

  public static boolean isNotCloudEnvironment(String projectId) {
    return getDatastoreEmulatorHost() != null &&
           projectId == null ||
           LOCAL_GAE_PROJECT_ID.equals(projectId) ||
           DEFAULT_OVERRIDE_LOCAL_GAE_PROJECT_ID.equals(projectId) ||
           TEST_PROJECT_ID.equals(projectId);
  }

  private static boolean isNotCloudEnvironment(DatastoreOptions options) {
    return isNotCloudEnvironment(options.getProjectId());
  }

  private static String getDatastoreEmulatorHost() {
    return System.getProperty("DATASTORE_EMULATOR_HOST", System.getenv("DATASTORE_EMULATOR_HOST"));
  }

}
