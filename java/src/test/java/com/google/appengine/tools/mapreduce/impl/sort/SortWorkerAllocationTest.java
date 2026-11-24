package com.google.appengine.tools.mapreduce.impl.sort;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class SortWorkerAllocationTest {

  static final String DISABLE_ALLOCATE_DIRECT_PROPERTY = SortWorker.class.getName() + ".disable_allocate_direct";

  @AfterEach
  void clearProperty() {
    System.clearProperty(DISABLE_ALLOCATE_DIRECT_PROPERTY);
  }

  @Test
  void triesDecreasingSizesDownToFloor() {
    SortWorker worker = new SortWorker(null, 1000);

    int previous = Integer.MAX_VALUE;
    for (int attempt = 0; attempt < 4; attempt++) {
      int size = worker.getMemoryForSort(attempt);
      assertTrue(size <= previous, "attempt " + attempt + " should not be larger than the previous one");
      assertTrue(size >= SortWorker.MIN_SORT_MEMORY_BYTES);
      previous = size;
    }
    assertTrue(worker.getMemoryForSort(3) < worker.getMemoryForSort(0));
  }

  @Test
  void neverAsksForMoreThanAnArrayCanHold() {
    SortWorker worker = new SortWorker(null, 1000);
    for (int attempt = 0; attempt < 4; attempt++) {
      assertTrue(worker.getMemoryForSort(attempt) <= SortWorker.MAX_SORT_MEMORY_BYTES);
    }
  }

  @Test
  void cappedByMaxMemory() {
    SortWorker worker = new SortWorker(1024L * 1024, 1000);
    for (int attempt = 0; attempt < 4; attempt++) {
      assertEquals(1024 * 1024, worker.getMemoryForSort(attempt));
    }
  }

  @Test
  void allocatesDirectByDefault() {
    ByteBuffer buffer = new SortWorker(8L * 1024 * 1024, 1000).allocateMemory();
    assertTrue(buffer.isDirect());
    assertEquals(8 * 1024 * 1024, buffer.capacity());
  }

  @Test
  void fallsBackToHeap() {
    System.setProperty(DISABLE_ALLOCATE_DIRECT_PROPERTY, "true");
    ByteBuffer buffer = new SortWorker(8L * 1024 * 1024, 1000).allocateMemory();
    assertFalse(buffer.isDirect());
    assertEquals(8 * 1024 * 1024, buffer.capacity());
  }
}
