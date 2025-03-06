package com.google.appengine.tools.pipeline.impl;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nullable;

/**
 * settings for how to asynchronously execute a task via a queue
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
@Getter @Setter
@ToString
public final class QueueSettings implements Cloneable {

  /**
   * name of the service to run the task on
   */
  @Nullable
  private String onService;

  /**
   * version of the service to run the task on
   */
  private String onServiceVersion;

  /**
   * name of the queue through which to enqueue the task
   */
  private String onQueue;

  /**
   * delay in seconds to set when enqueueing the task (eg, should not execute until *at least* this much time has passed
   */
  private Long delayInSeconds;

  /**
   * Merge will override any {@code null} setting with a matching setting from {@code other}.
   * Note, delay value is not being merged.
   */
  public QueueSettings merge(QueueSettings other) {
    if (onService == null) {
      onService = other.getOnService();
      onServiceVersion = other.getOnServiceVersion();
    }
    if (onQueue == null) {
      onQueue = other.getOnQueue();
    }
    return this;
  }

  @Override
  public QueueSettings clone() {
    try {
      return (QueueSettings) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Should never happen", e);
    }
  }
}
