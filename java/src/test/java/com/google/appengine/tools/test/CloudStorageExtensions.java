package com.google.appengine.tools.test;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import org.junit.jupiter.api.extension.*;

import java.io.ByteArrayInputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Parameter;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Copyright 2025 Worklytics, Co.
 *
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({
	CloudStorageExtension.class,
	CloudStorageExtension.ParameterResolver.class,
})
public @interface CloudStorageExtensions {

}

class CloudStorageExtension implements BeforeAllCallback {

	public static final String KEY_ENV_VAR = "CI_SERVICE_ACCOUNT_KEY";

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		String keyVar = System.getenv(KEY_ENV_VAR);
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
			String base64EncodedServiceAccountKey = keyVar.trim();
			String jsonKey = new String(Base64.getDecoder().decode(base64EncodedServiceAccountKey.getBytes()));

			credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(jsonKey.getBytes()));
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

	static List<BucketInfo.LifecycleRule> defaultTestDataLifecycle() {
		return Collections.singletonList(new BucketInfo.LifecycleRule(
			BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction(),
			BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(30).build()));
	}

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
