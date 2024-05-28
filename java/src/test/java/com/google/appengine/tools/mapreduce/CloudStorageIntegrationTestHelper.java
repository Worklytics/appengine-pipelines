package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.development.testing.LocalServiceTestConfig;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageFactory;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * sets up storage bucket for tests
 *
 * as of Apr 2020, no gcs emulator https://cloud.google.com/sdk/gcloud/reference/beta/emulators
 *
 * @see "https://googleapis.dev/java/google-cloud-storage/1.106.0/com/google/cloud/storage/testing/RemoteStorageHelper.html"
 *
 * eg,
 *   1. create SA in target project; give it "Storage Admin" role
 *   2. cat ~/Downloads/worklytics-ci-111242f427df.json | base64
 *   3. set output of that as your env variable
 *    - in IntelliJ, set this via RunConfigurations --> Env Variables.
 *    - in GitHub, set it as via repo --> Settings --> Secrets so it can be utilized in workflows
 *
 * q: better to have a helper class for this? analogous to
 * @see com.google.appengine.tools.development.testing.LocalServiceTestHelper
 */
public class CloudStorageIntegrationTestHelper implements LocalServiceTestConfig {

  public final String KEY_ENV_VAR = "APPENGINE_MAPREDUCE_CI_SERVICE_ACCOUNT_KEY";

  @Getter
  Storage storage;
  @Getter
  static String bucket;
  @Getter
  String projectId;
  @Getter
  Credentials credentials;

  @Getter
  String base64EncodedServiceAccountKey;

  @SneakyThrows
  @Override
  public void setUp() {

    String keyVar = System.getenv(KEY_ENV_VAR);


    if (keyVar == null) {
      //attempt w default credentials
      credentials = StorageOptions.getDefaultInstance().getCredentials();

      //TODO: more elegant solution? weirdness seems to happen if mix projects; credentials' project
      // isn't exposed to java code via any public interface; yet bucket is created in the project
      // to which the credentials default project is set
      projectId = "worklytics-ci";
      //throw new IllegalStateException("Must set environment variable " + KEY_ENV_VAR + " as base64 encoded service account key to use for storage integration tests");
    } else {
      base64EncodedServiceAccountKey = keyVar.trim();
      String jsonKey = new String(Base64.getDecoder().decode(base64EncodedServiceAccountKey.getBytes()));

      credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(jsonKey.getBytes()));
      projectId = ((ServiceAccountCredentials) credentials).getProjectId();
    }

    storage = StorageOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(projectId)
      .build().getService();
    if (bucket == null) {
      bucket = RemoteStorageHelper.generateBucketName();
      storage.create(BucketInfo.of(bucket));

      //delete bucket at shutdown

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          RemoteStorageHelper.forceDelete(storage, bucket, 5, TimeUnit.SECONDS);
        } catch (Throwable e) {
          Logger.getAnonymousLogger().log(Level.WARNING, "Failed to cleanup bucket: " + bucket);
        }
      }));
    }
  }

  @Deprecated //attach delete to global runtime shutdown
  @SneakyThrows
  @Override
  public void tearDown() {
  }
}
