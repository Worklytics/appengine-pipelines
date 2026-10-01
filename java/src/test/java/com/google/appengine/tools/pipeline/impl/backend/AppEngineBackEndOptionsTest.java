package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOpenTelemetryOptions;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class AppEngineBackEndOptionsTest {

  @SneakyThrows
  @Test
  void getOptions() {
    //TODO: replace this with GoogleCredentials.getApplicationDefault() when it's available, if we ever auth the Github
    // Action with GCP (debatably necessary for integration tests)
    GoogleCredentials credentials = GoogleCredentials.newBuilder()
      // use some non-local/test project id to not override the credentials
      .setQuotaProjectId("some-project")
      .setAccessToken(AccessToken.newBuilder().setTokenValue("token").setExpirationTime(new Date()).build())
      .build();


    Datastore datastore = DatastoreOptions.newBuilder()
      .setProjectId(credentials.getQuotaProjectId())
      .setCredentials(credentials)
      .setOpenTelemetryOptions(DatastoreOpenTelemetryOptions.newBuilder().build())
      .build().getService();

    AppEngineBackEnd backend = new AppEngineBackEnd(datastore, mock(PipelineTaskQueue.class), mock(AppEngineServicesService.class));

    assertEquals(datastore.getOptions().getProjectId(),
      backend.getOptions().as(AppEngineBackEnd.Options.class).getProjectId());

    assertEquals(datastore.getOptions().getCredentials(),
      backend.getOptions().as(AppEngineBackEnd.Options.class).getCredentials());


    assertFalse(datastore.getOptions().getCredentials() instanceof NoCredentials);

    assertTrue(
      datastore.getOptions().getCredentials() instanceof UserCredentials //local case
      || datastore.getOptions().getCredentials() instanceof ServiceAccountCredentials //ci case
      || datastore.getOptions().getCredentials() instanceof GoogleCredentials //ci case (w/o OIDC to authenticate GitHub action runner)
    );

    //survives roundtrip serialization
    byte[] serialized = SerializationUtils.serialize(backend.getOptions());

    AppEngineBackEnd.Options deserialized = (AppEngineBackEnd.Options) SerializationUtils.deserialize(serialized);
    AppEngineBackEnd fresh = new AppEngineBackEnd(deserialized, mock(PipelineTaskQueue.class), mock(AppEngineServicesService.class));

    assertEquals(
      backend.getOptions().as(AppEngineBackEnd.Options.class).getProjectId(),
      fresh.getOptions().as(AppEngineBackEnd.Options.class).getProjectId());
    assertEquals(
      backend.getOptions().as(AppEngineBackEnd.Options.class).getCredentials(),
      fresh.getOptions().as(AppEngineBackEnd.Options.class).getCredentials());
  }

  @Test
  void datastoreForKeyUsesTheKeyDatabase() {
    Datastore datastore = DatastoreOptions.newBuilder()
        .setProjectId("test-project")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
    AppEngineBackEnd backend = new AppEngineBackEnd(datastore, mock(PipelineTaskQueue.class),
        mock(AppEngineServicesService.class));

    Key defaultKey = Key.newBuilder("test-project", "JobRecord", "root").build();
    Key namedKey = Key.newBuilder("test-project", "JobRecord", "root").setDatabaseId("tenant-db").build();
    Key namespacedKey = Key.newBuilder("test-project", "JobRecord", "root").setNamespace("tenant-ns").build();

    assertSame(datastore, backend.datastoreForKey(defaultKey));
    assertEquals("tenant-db", backend.datastoreForKey(namedKey).getOptions().getDatabaseId());
    assertEquals("tenant-ns", backend.datastoreForKey(namespacedKey).getOptions().getNamespace());
    assertSame(backend.datastoreForKey(namedKey), backend.datastoreForKey(namedKey));
    assertSame(backend.datastoreForKey(namespacedKey), backend.datastoreForKey(namespacedKey));
  }
}
