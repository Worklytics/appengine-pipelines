package com.google.appengine.tools.pipeline.impl.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import dagger.ObjectGraph;
import lombok.NonNull;
import lombok.extern.java.Log;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper to get an ObjectGraph out of a Dagger Module.
 * <p>
 * This is mostly intended to be used for jobs, thus Default Module is linked somehow to AsyncModule.
 *
 */

@Log
public class DIUtil {


	private static final Object lock = new Object();

	private static final Map<String, ObjectGraph> objectGraphCache = new ConcurrentHashMap<>(1);

	private static final Map<String, ObjectGraph> overriddenObjectGraphCache = new ConcurrentHashMap<>(1);

	// just for logging purposes
	private static boolean overridden = false;

	/**
	 * Injects and Injectable instance through Reflection
	 * @param injectable class to inject
	 */
	public static void inject(String moduleClassFQN, Object injectable) {
		ObjectGraph objectGraph = getFromModuleClass(moduleClassFQN);
		objectGraph.inject(injectable);
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
	 * @param moduleFQNClassName module fully qualified name
	 * @return the object graph for that module
	 * @throws RuntimeException if the class is not a Dagger Module or doesn't meet the requirements
	 */
	public static ObjectGraph getFromModuleClass(@NonNull String moduleFQNClassName) {
		synchronized (lock) {
			if (overridden) {
				return overriddenObjectGraphCache.getOrDefault(moduleFQNClassName, objectGraphCache.get(moduleFQNClassName));
			}
			try {
				Class<?> moduleClass = Class.forName(moduleFQNClassName);
				Preconditions.checkArgument(moduleClass.isAnnotationPresent(dagger.Module.class), "Class is not a Dagger Module: " + moduleFQNClassName);

				Method getObjectGraphMethod = moduleClass.getMethod("getObjectGraph");

				Preconditions.checkArgument(Modifier.isStatic(getObjectGraphMethod.getModifiers()), "getObjectGraph is not static");
				Preconditions.checkArgument(getObjectGraphMethod.getReturnType().equals(ObjectGraph.class), "getObjectGraph does not return an ObjectGraph!");

				ObjectGraph objectGraph = (ObjectGraph) getObjectGraphMethod.invoke(null);

				objectGraphCache.put(moduleFQNClassName, objectGraph);
				return objectGraph;
			} catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new RuntimeException("Couldn't get an object graph " + e.getMessage(), e);
			}
		}
	}

	/**
	 * A test helper to directly override the default module with the test's graph
	 * @param graph the object graph to override for default module
	 */
	@VisibleForTesting
	public static void overrideGraphForTests(String key, ObjectGraph graph) {
		synchronized (lock) {
			overriddenObjectGraphCache.put(key, graph);
			overridden = true;
		}
	}

	/**
	 * Resets the objectGraphCache to its original state (intended to be used on tests' teardown)
	 */
	@VisibleForTesting
	public static void resetGraph() {
		synchronized (lock) {
			overriddenObjectGraphCache.clear();
			overridden = false;
			log.fine("Reset overridden DI Module");
		}
	}
}
