package com.google.appengine.tools.pipeline.impl.util;

import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.protobuf.ByteString;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;

@Singleton
public class KmsService {
  private final KeyManagementServiceClient client;

  @Inject
  public KmsService() {
    try {
      this.client = KeyManagementServiceClient.create();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create KeyManagementServiceClient", e);
    }
  }

  public byte[] encrypt(String keyName, byte[] plaintext) {
    EncryptResponse response = client.encrypt(keyName, ByteString.copyFrom(plaintext));
    return response.getCiphertext().toByteArray();
  }

  public byte[] decrypt(String keyName, byte[] ciphertext) {
    DecryptResponse response = client.decrypt(keyName, ByteString.copyFrom(ciphertext));
    return response.getPlaintext().toByteArray();
  }
}
