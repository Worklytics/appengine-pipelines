// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;


/**
 * Context for each shard.
 */
public interface ShardContext extends Context {

  /**
   * @return the total number of shards.
   */
  int getShardCount();

  /**
   * @return the number of this mapper or reducer shard (zero-based).
   */
  int getShardNumber();

  /**
   * @return a {@link Counters} object for doing simple aggregate calculations.
   */
  Counters getCounters();

  /**
   * @param name of counter to get
   * @return the {@link Counter} with the given name.
   */
  Counter getCounter(String name);

  /**
   * Increments the {@link Counter} with the given name by {@code delta}.
   * @param name identifies the counter to increment
   * @param delta amount by which to increment counter
   */
  void incrementCounter(String name, long delta);

  /**
   * Increments the {@link Counter} with the given name by 1.
   * @param name identifies the counter to increment
   */
  void incrementCounter(String name);
}
