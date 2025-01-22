/**
 * Copyright 2025 Worklytics, Co.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */
package com.google.appengine.tools.test;

import org.junit.jupiter.api.extension.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({
	CloudStorageExtension.class,
	CloudStorageExtension.ParameterResolver.class,
})
public @interface CloudStorageExtensions {

}

