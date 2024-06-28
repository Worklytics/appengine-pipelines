// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;


/**
 * Context for each worker (mapper or reducer) shard.
 *
 * @param <O> type of output values produced by the worker
 */
public interface WorkerContext<O> extends ShardContext {

  /**
   * Emits a value to the output.
   * @param value the value to output
   */
  void emit(O value);
}
