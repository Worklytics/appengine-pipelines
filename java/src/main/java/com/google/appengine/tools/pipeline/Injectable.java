package com.google.appengine.tools.pipeline;

/**
 * equivalent of Spring component, in a sense
 *
 * FQN of Dagger 1 module class that should be used when filling this classes dependencies, if any.
 *
 *
 * Usage:
 *
 * @Injectable("com.google.appengine.tools.pipeline.impl.backend.dagger.AsyncModule")
 * public class MyJob extends Job0<Void> {
 *
 *   @Inject transient SomeDependency someDependency;
 * }
 *
 * NOTE: you must still register it in injects of the Module!!
 */
public @interface Injectable {

  String value() default "";

}
