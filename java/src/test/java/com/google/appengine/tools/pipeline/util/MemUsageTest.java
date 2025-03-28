package com.google.appengine.tools.pipeline.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MemUsageTest {

  @Test
  void getMemoryUsagePercentage() {
    MemUsage memUsage = MemUsage.builder().build();

    assertTrue(memUsage.getMemoryUsagePercentage() > 0);
    assertTrue(memUsage.getMemoryUsagePercentage() < 1);

    System.out.println(memUsage.getMemoryUsagePercentage());
  }

  @Test
  void getMemoryUsage() {
    MemUsage memUsage = MemUsage.builder().build();

    assertTrue(memUsage.getMemoryUsage() > 0);
    assertTrue(memUsage.getMemoryUsage() < 5000);
    System.out.println(memUsage.getMemoryUsage());
  }
}