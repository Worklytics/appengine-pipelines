package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class AppEngineBackEndOptionsTest {

  @SneakyThrows
  @Test
  void getOptions() {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    Datastore datastore = DatastoreOptions.newBuilder()
      .setProjectId(credentials.getQuotaProjectId())
      .setCredentials(credentials)
      .build().getService();

    AppEngineBackEnd backend = new AppEngineBackEnd(datastore, new AppEngineTaskQueue());

    assertEquals(datastore.getOptions().getProjectId(),
      backend.getOptions().as(AppEngineBackEnd.Options.class).getProjectId());

    assertEquals(datastore.getOptions().getCredentials(),
      backend.getOptions().as(AppEngineBackEnd.Options.class).getCredentials());


    assertFalse(datastore.getOptions().getCredentials() instanceof NoCredentials);

    assertTrue(
      datastore.getOptions().getCredentials() instanceof UserCredentials //local case
      || datastore.getOptions().getCredentials() instanceof ServiceAccountCredentials //ci case
    );

    //survives roundtrip serialization
    byte[] serialized = SerializationUtils.serialize(backend.getOptions());

    AppEngineBackEnd.Options deserialized = (AppEngineBackEnd.Options) SerializationUtils.deserialize(serialized);
    AppEngineBackEnd fresh = new AppEngineBackEnd(deserialized);

    assertEquals(
      backend.getOptions().as(AppEngineBackEnd.Options.class).getProjectId(),
      fresh.getOptions().as(AppEngineBackEnd.Options.class).getProjectId());
    assertEquals(
      backend.getOptions().as(AppEngineBackEnd.Options.class).getCredentials(),
      fresh.getOptions().as(AppEngineBackEnd.Options.class).getCredentials());
  }
}
