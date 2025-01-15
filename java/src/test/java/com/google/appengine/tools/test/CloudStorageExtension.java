package com.google.appengine.tools.test;


import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
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
  */
public class CloudStorageExtension implements BeforeAllCallback, BeforeEachCallback {

  public static final String KEY_ENV_VAR = "CI_SERVICE_ACCOUNT_KEY";

  public static String getBase64EncodedServiceAccountKey() {
    return System.getenv(KEY_ENV_VAR);
  }

  static Optional<ServiceAccountCredentials> getServiceAccountCredentials() {
    return Optional.ofNullable(getBase64EncodedServiceAccountKey())
      .map(keyVar -> {
        String base64EncodedServiceAccountKey = keyVar.trim();
        String jsonKey = new String(Base64.getDecoder().decode(base64EncodedServiceAccountKey.getBytes()));
        try {
          return ((ServiceAccountCredentials) ServiceAccountCredentials.fromStream(new ByteArrayInputStream(jsonKey.getBytes())));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
  }

  public static String getProjectId() {
    return getServiceAccountCredentials().map(ServiceAccountCredentials::getProjectId).orElse("worklytics-ci");
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    String keyVar = getBase64EncodedServiceAccountKey();
    Credentials credentials;
    String projectId;
    if (keyVar == null) {
      //attempt w default credentials
      credentials = StorageOptions.getDefaultInstance().getCredentials();

      //TODO: more elegant solution? weirdness seems to happen if mix projects; credentials' project
      // isn't exposed to java code via any public interface; yet bucket is created in the project
      // to which the credentials default project is set
      projectId = "worklytics-ci";
      //throw new IllegalStateException("Must set environment variable " + KEY_ENV_VAR + " as base64 encoded service account key to use for storage integration tests");
    } else {
      credentials = getServiceAccountCredentials().get();
      projectId = ((ServiceAccountCredentials) credentials).getProjectId();
    }

    Storage storage = StorageOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(projectId)
      .build().getService();

    context.getStore(ExtensionContext.Namespace.create(CloudStorageExtension.class)).put(Storage.class, storage);

    String bucket = context.getStore(ExtensionContext.Namespace.create(CloudStorageExtension.class)).get("bucket", String.class);
    if (bucket == null) {
      bucket = RemoteStorageHelper.generateBucketName();
      context.getStore(ExtensionContext.Namespace.create(CloudStorageExtension.class)).put("bucket", bucket);

      //avoid test data being retained forever, even if bucket deletion post-test fails
      storage.create(BucketInfo.newBuilder(bucket)
        .setLifecycleRules(defaultTestDataLifecycle())
        .setSoftDeletePolicy(null) // no soft-deletion
        .build());

      //delete bucket at shutdown
      final String bucketToDelete = bucket;
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          RemoteStorageHelper.forceDelete(storage, bucketToDelete, 5, TimeUnit.SECONDS);
        } catch (Throwable e) {
          Logger.getAnonymousLogger().log(Level.WARNING, "Failed to cleanup bucket: " + bucketToDelete);
        }
      }));

    }
  }

  /**
   * file any String-type parameter named 'bucket' with the random bucket name
   *
   * @param context the current extension context; never {@code null}
   */
  @SneakyThrows
  @Override
  public void beforeEach(ExtensionContext context) {
    String bucket = context.getStore(ExtensionContext.Namespace.create(CloudStorageExtension.class)).get("bucket", String.class);
    Field f = Arrays.stream(context.getRequiredTestInstance().getClass().getDeclaredFields())
      .filter(field -> field.getType().equals(String.class) && field.getName().equals("bucket"))
      .findAny()
      .orElse(null);
    if (f != null) {
      f.setAccessible(true);
      f.set(context.getRequiredTestInstance(), bucket);
    }
  }

  static List<BucketInfo.LifecycleRule> defaultTestDataLifecycle() {
    return Collections.singletonList(new BucketInfo.LifecycleRule(
      BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction(),
      BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(30).build()));
  }

  /**
   * files any Storage-type parameter in the test class with the Storage instance created in the beforeAll method
   */
  public static class ParameterResolver implements org.junit.jupiter.api.extension.ParameterResolver {

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
      return extensionContext.getStore(ExtensionContext.Namespace.create(CloudStorageExtension.class)).get(Storage.class);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
      Parameter parameter = parameterContext.getParameter();
      return Storage.class.equals(parameter.getType());
    }
  }

}