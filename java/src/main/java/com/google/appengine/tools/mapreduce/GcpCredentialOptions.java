package com.google.appengine.tools.mapreduce;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.client.googleapis.services.GoogleClientRequestInitializer;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.googleapis.services.json.CommonGoogleJsonClientRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Optional;

public interface GcpCredentialOptions {

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

  //helper util; consider moving to GCPUtils class, or something ...
  static Storage getStorageClient(@Nullable GcpCredentialOptions gcpCredentialOptions) {
    Credentials credentials = determineCredentials(gcpCredentialOptions)
      .orElseGet(() -> StorageOptions.getDefaultInstance().getCredentials());

    return StorageOptions.newBuilder()
      .setCredentials(credentials)
      .build().getService();
  }
  static Optional<Credentials> determineCredentials(@Nullable GcpCredentialOptions gcpCredentialOptions) {
    return Optional.ofNullable(gcpCredentialOptions)
      .map(gcp -> gcp.getServiceAccountCredentials()).orElse(Optional.empty())
      .map(c -> (Credentials) c);
  }
}
