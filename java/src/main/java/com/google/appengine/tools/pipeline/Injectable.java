package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.di.DaggerJobRunServiceComponent;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * equivalent of Spring component, in a sense
 *
 * FQN of Dagger 2 container class that should be used when filling this classes dependencies, if any.
 *
 *
 * Usage:
 *
 * @Injectable(DaggerAsyncContainer.class)
 * public class MyJob extends Job0<Void> {
 *
 *   @Inject transient SomeDependency someDependency;
 * }
 *
 * NOTE: you must still register it in injects of the Module!!
 *
 * q: good approach? probably need some way to lazily provide the module, right?
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Injectable {

  Class<?> value() default DaggerJobRunServiceComponent.class;

}
