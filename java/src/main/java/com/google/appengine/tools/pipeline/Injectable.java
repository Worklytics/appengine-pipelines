package com.google.appengine.tools.pipeline;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * equivalent of Spring component, in a sense
 *
 * FQN of Dagger 1 module class that should be used when filling this classes dependencies, if any.
 *
 *
 * Usage:
 *
 * @Injectable(AsyncModule.class)
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

  Class<?> value() default DefaultDIModule.class;

}
