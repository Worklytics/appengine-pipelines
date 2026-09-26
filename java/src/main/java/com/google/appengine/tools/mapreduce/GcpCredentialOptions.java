package com.google.appengine.tools.mapreduce;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Optional;

public interface GcpCredentialOptions {

  /**
   * When set (for example {@code http://localhost:4443}), storage clients talk to that
   * server and do not exchange a service-account token.
   */
  String STORAGE_EMULATOR_HOST = "STORAGE_EMULATOR_HOST";

  String getServiceAccountKey();

  /**
   * @return Credentials to use when accessing GCS bucket
   * @throws IOException if can't parse ServiceAccountCredentials from serviceAccountKey
   */
  @JsonIgnore
  @SneakyThrows //having this explicitly throw checked IOException is annoying, not especially useful
  default Optional<ServiceAccountCredentials> getServiceAccountCredentials() {
    if (getServiceAccountKey() == null) {
      return Optional.empty();
    } else {
      String jsonKey = new String(Base64.getDecoder().decode(getServiceAccountKey().trim().getBytes()));
      return Optional.of(ServiceAccountCredentials.fromStream(new ByteArrayInputStream(jsonKey.getBytes())));
    }
  }

  static Optional<String> storageEmulatorHost() {
    String host = System.getenv(STORAGE_EMULATOR_HOST);
    if (host == null || host.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(host.trim());
  }

  static Storage emulatorStorage(String host, String projectId) {
    return StorageOptions.newBuilder()
      .setHost(host)
      .setProjectId(projectId)
      .setCredentials(NoCredentials.getInstance())
      .build()
      .getService();
  }

  //helper util; consider moving to GCPUtils class, or something ...
  static Storage getStorageClient(@Nullable GcpCredentialOptions gcpCredentialOptions) {
    Optional<String> emulatorHost = storageEmulatorHost();
    if (emulatorHost.isPresent()) {
      return emulatorStorage(emulatorHost.get(), "test-project");
    }

    Credentials credentials = determineCredentials(gcpCredentialOptions)
      .orElseGet(() -> StorageOptions.getDefaultInstance().getCredentials());

    return StorageOptions.newBuilder()
      .setCredentials(credentials)
      .build().getService();
  }
  static Optional<Credentials> determineCredentials(@Nullable GcpCredentialOptions gcpCredentialOptions) {
    return Optional.ofNullable(gcpCredentialOptions)
      .flatMap(GcpCredentialOptions::getServiceAccountCredentials)
      .map(c -> c);
  }
}
