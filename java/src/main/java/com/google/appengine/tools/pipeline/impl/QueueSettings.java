package com.google.appengine.tools.pipeline.impl;

/**
 * Queue settings implementation.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public final class QueueSettings implements Cloneable {

  private String onService;
  private String onServiceVersion;
  private String onQueue;
  private Long delay;

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


  public String getOnService() {
    return this.onService;
  }

  public QueueSettings setOnService(String onService) {
    this.onService = onService;
    return this;
  }

  public QueueSettings setOnServiceVersion(String serviceVersion) {
    this.onServiceVersion = serviceVersion;
    return this;
  }

  public String getOnServiceVersion() {
    return onServiceVersion;
  }

  public QueueSettings setOnQueue(String onQueue) {
    this.onQueue = onQueue;
    return this;
  }

  public String getOnQueue() {
    return onQueue;
  }

  public void setDelayInSeconds(Long delay) {
    this.delay = delay;
  }

  public Long getDelayInSeconds() {
    return delay;
  }

  @Override
  public QueueSettings clone() {
    try {
      return (QueueSettings) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Should never happen", e);
    }
  }

  @Override
  public String toString() {
    return "QueueSettings[onService=" + onService + ", onServiceVersion="
        + onServiceVersion + ", onQueue=" + onQueue + ", delayInSeconds=" + delay + "]";
  }
}
