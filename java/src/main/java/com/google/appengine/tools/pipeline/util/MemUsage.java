package com.google.appengine.tools.pipeline.util;

import lombok.Builder;
import lombok.NonNull;

/**
 * Utility class to check memory usage.
 */
@Builder
public class MemUsage {

  public final static Double DEFAULT_THRESHOLD = 0.9;

  /**
   * Threshold to consider memory usage as high.
   *
   * Set above 1.0 to disable the check.
   */
  @Builder.Default
  private Double threshold =DEFAULT_THRESHOLD;

  public boolean isMemoryUsageHigh() {
    return getMemoryUsagePercentage() > threshold ;
  }

  Double getMemoryUsagePercentage() {
    Runtime runtime = Runtime.getRuntime();
    long totalMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();
    long maxMemory = runtime.maxMemory();
    return (double) (totalMemory - freeMemory) / maxMemory;
  }

  public Integer getMemoryUsage() {
    Runtime runtime = Runtime.getRuntime();
    long totalMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();
    return (int) (totalMemory - freeMemory) / (1024 * 1024);
  }
}
