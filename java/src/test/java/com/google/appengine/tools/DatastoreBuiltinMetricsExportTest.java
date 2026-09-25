package com.google.appengine.tools;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.DatastoreOpenTelemetryOptions;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.telemetry.BuiltInDatastoreMetricsProvider;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;

/**
 * Datastore 3.0.0 started a private PeriodicMetricReader even when builtin Cloud Monitoring
 * export was off. 3.2.0 and later return {@link OpenTelemetry#noop()} unless that export is
 * enabled. This library must keep the export off, including {@link DatastoreOptions#getDefaultInstance()}.
 */
class DatastoreBuiltinMetricsExportTest {

  @Test
  void defaultInstanceKeepsBuiltinMetricsExportOff() {
    DatastoreOptions options = DatastoreOptions.getDefaultInstance();

    assertExportOff(options);
  }

  @Test
  void defaultOpenTelemetryOptionsKeepBuiltinMetricsExportOff() {
    DatastoreOptions options = DatastoreOptions.newBuilder()
      .setProjectId("test-project")
      .setCredentials(NoCredentials.getInstance())
      .setOpenTelemetryOptions(DatastoreOpenTelemetryOptions.newBuilder().build())
      .build();

    assertExportOff(options);
  }

  private static void assertExportOff(DatastoreOptions options) {
    assertFalse(options.getOpenTelemetryOptions().isExportBuiltinMetricsToGoogleCloudMonitoring());
    assertSame(
      OpenTelemetry.noop(),
      BuiltInDatastoreMetricsProvider.INSTANCE.createOpenTelemetry(options));
  }
}
