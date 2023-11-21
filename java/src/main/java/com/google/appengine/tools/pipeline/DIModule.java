package com.google.appengine.tools.pipeline;

/**
 * FQN of Dagger 1 module class that should be used when filling this classes dependencies, if any.
 *
 * Usage:
 *
 * @DIModule("com.google.appengine.tools.pipeline.impl.backend.dagger.AsyncModule")
 * public class MyJob extends Job0<Void> {
 *
 *   @Inject transient SomeDependency someDependency;
 * }
 */
public @interface DIModule {

  String value() default "";

}
