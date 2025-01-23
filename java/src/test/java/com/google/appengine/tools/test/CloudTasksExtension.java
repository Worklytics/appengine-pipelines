package com.google.appengine.tools.test;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Parameter;

/**
 * Copyright 2025 Worklytics, Co.
 *
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */
public class CloudTasksExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback {

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		LocalServiceTestHelper helper =
			new LocalServiceTestHelper(
				new LocalTaskQueueTestConfig().setDisableAutoTaskExecution(true),
				new LocalModulesServiceTestConfig() //yeah, this is still here ...
			);
		context.getStore(ExtensionContext.Namespace.create(getClass())).put(LocalServiceTestHelper.class, helper);
	}

	@Override
	public void beforeEach(ExtensionContext context) throws Exception {
		LocalServiceTestHelper helper =
			(LocalServiceTestHelper) context.getStore(ExtensionContext.Namespace.create(getClass())).get(LocalServiceTestHelper.class);

		helper.setUp();

		LocalTaskQueue taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();

		context.getStore(ExtensionContext.Namespace.create(getClass())).put(LocalTaskQueue.class, taskQueue);
	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		LocalServiceTestHelper helper =
			(LocalServiceTestHelper) context.getStore(ExtensionContext.Namespace.create(getClass())).get(LocalServiceTestHelper.class);

		helper.tearDown();
	}

  public static class ParameterResolver implements org.junit.jupiter.api.extension.ParameterResolver  {
    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
      return extensionContext.getStore(ExtensionContext.Namespace.create(CloudTasksExtension.class)).get(LocalTaskQueue.class );
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
      Parameter parameter = parameterContext.getParameter();
      return LocalTaskQueue.class.equals(parameter.getType());
    }
  }
}
