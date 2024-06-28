package com.google.appengine.tools.pipeline.impl.util;

/**
 * Copyright Worklytics, Co. 2024.
 */
import com.google.appengine.tools.pipeline.Injectable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import lombok.NonNull;
import lombok.extern.java.Log;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper to get a component instance out of Dagger, support runtime injection of classes.
 * <p>
 *
 */
@Log
public class DIUtil {


	private static final Object lock = new Object();

	private static final Map<Class<?>, Object> componentCache = new ConcurrentHashMap<>(1);

	private static final Map<Class<?>, Object> overriddenComponentCache = new ConcurrentHashMap<>(1);

	// just for logging purposes
	private static boolean overridden = false;

  /**
   * Injects an instance annotated with @Injectable, based on the module provided in the annotation
   *
   * @param instance
   */
  public static void inject(Object instance) {
    Optional<Injectable> injectable = Optional.ofNullable(instance.getClass().getAnnotation(Injectable.class));

    if (injectable.isPresent()) {
      inject(injectable.get().value(), instance);
    }
  }

  public static boolean isInjectable(Object instance) {
    return instance.getClass().getAnnotation(Injectable.class) != null;
  }

	/**
	 * Injects and Injectable instance through Reflection
   * @param componentClass component class (Dagger-generated)
	 * @param instance class instance to inject
	 */
	public static void inject(Class<?> componentClass, Object instance) {
    Object objectGraph = getFromComponentClass(componentClass);

    try {
      Method injectMethod = objectGraph.getClass().getMethod("inject", instance.getClass());
      injectMethod.setAccessible(true); //avoid 'volatile' thing
      injectMethod.invoke(objectGraph, instance);
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException("Couldn't inject object " + e.getMessage(), e);
    }
	}

	/**
	 * Helper to get an ObjectGraph out of a Dagger Module through reflection
	 * Synchronized to avoid concurrency in cache access/write from same instance.
	 * This also should speed up object graph instantiation in classes.
	 * <p>
	 * Requirements:
	 * - Module has a static instance of the graph it builds and provided by method of signature
	 *   public static ObjectGraph getObjectGraph()
	 *
	 * @param componentClass component fully qualified name
	 * @return the object graph for that module
	 * @throws RuntimeException if the class is not a Dagger Module or doesn't meet the requirements
	 */
	 static Object getFromComponentClass(@NonNull Class<?> componentClass) {
    synchronized (lock) {
      if (overridden && overriddenComponentCache.containsKey(componentClass)) {
        return overriddenComponentCache.get(componentClass);
      }
      if (componentCache.containsKey(componentClass)) {
        return componentCache.get(componentClass);
      }
      try {
        Object component = componentClass.getMethod("create").invoke(null);
        componentCache.put(componentClass, component);
        return component;
      } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
        throw new RuntimeException("Couldn't get an object graph " + e.getMessage(), e);
      }
    }
  }


  @VisibleForTesting
  public static void overrideComponentInstanceForTests(Class<?> clazz, Object componentInstance) {
    synchronized (lock) {
      overriddenComponentCache.put(clazz, componentInstance);
      overridden = true;
    }
  }

	/**
	 * Resets the componentCache to its original state (intended to be used on tests' teardown)
	 */
	@VisibleForTesting
	public static void resetComponents() {
		synchronized (lock) {
			overriddenComponentCache.clear();
			overridden = false;
			log.fine("Reset overridden DI Module");
		}
	}
}
