// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_MAP_FANOUT;
import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_MERGE_FANIN;
import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_SORT_BATCH_PER_EMIT_BYTES;
import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_SORT_READ_TIME_MILLIS;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_MILLIS_PER_SLICE;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SHARD_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_RETRIES;
import static org.junit.jupiter.api.Assertions.*;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 */
@SuppressWarnings("deprecation")
public class MapReduceSettingsTest {

  private final LocalServiceTestHelper helper = // work around for b/17977352
      new LocalServiceTestHelper(new LocalTaskQueueTestConfig().setDisableAutoTaskExecution(true));

  @BeforeEach
  public void setUp() {
    helper.setUp();
  }

  @AfterEach
  public void tearDown() {
    helper.tearDown();
  }

  @Test
  public void testDefaultSettings() {
    MapReduceSettings mrSettings = MapReduceSettings.builder()
      .bucketName("app_default_bucket")
      .build();
    assertEquals(DEFAULT_BASE_URL, mrSettings.getBaseUrl());
    assertEquals(DEFAULT_MAP_FANOUT, mrSettings.getMapFanout());
    assertEquals(DEFAULT_SHARD_RETRIES, mrSettings.getMaxShardRetries());
    assertEquals(DEFAULT_SLICE_RETRIES, mrSettings.getMaxSliceRetries());
    assertNull(mrSettings.getMaxSortMemory());
    assertEquals(DEFAULT_MERGE_FANIN, mrSettings.getMergeFanin());
    assertEquals(DEFAULT_MILLIS_PER_SLICE, mrSettings.getMillisPerSlice());
    assertEquals(null, mrSettings.getModule());
    assertEquals(DEFAULT_SORT_BATCH_PER_EMIT_BYTES, mrSettings.getSortBatchPerEmitBytes());
    assertEquals(DEFAULT_SORT_READ_TIME_MILLIS, mrSettings.getSortReadTimeMillis());
    assertNull(mrSettings.getWorkerQueueName());
  }

  @Test
  public void testNonDefaultSettings() {
    MapReduceSettings.MapReduceSettingsBuilder builder = MapReduceSettings.builder();
    builder.module("m");
    builder = builder.baseUrl("base-url");
    builder = builder.bucketName("bucket");
    try {
      builder.mapFanout(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.mapFanout(3);
    try {
      builder.maxShardRetries(-1);
      fail("IllegalArgumentException expected for negative maxShardRetries");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.maxShardRetries(1);
    try {
      builder.maxSliceRetries(-1);
      fail("IllegalArgumentException expected for negative maxSliceRetries");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.maxSliceRetries(0);
    try {
      builder.maxSortMemory(-1L);
      fail("IllegalArgumentException expected for negative maxSortMemory");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.maxSortMemory(10L);
    try {
      builder.mergeFanin(-1);
      fail("IllegalArgumentException expected for negative mergeFanin");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.mergeFanin(4);
    builder = builder.millisPerSlice(10);
    try {
      builder.sortBatchPerEmitBytes(-1);
      fail("IllegalArgumentException expected for negative sortBatchPerEmitBytes");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.sortBatchPerEmitBytes(5);
    try {
      builder.sortReadTimeMillis(-1);
      fail("IllegalArgumentException expected for negative sortReadTimeMillis");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.sortReadTimeMillis(6);
    builder = builder.workerQueueName("queue1");


    MapReduceSettings mrSettings = builder.build();
    //assertEquals("b1", mrSettings.getModule());
    //assertNull(mrSettings.getModule());
    assertEquals("bucket", mrSettings.getBucketName());
    assertEquals("base-url", mrSettings.getBaseUrl());
    assertEquals(3, mrSettings.getMapFanout());
    assertEquals(1, mrSettings.getMaxShardRetries());
    assertEquals(0, mrSettings.getMaxSliceRetries());
    assertEquals(10L, (long) mrSettings.getMaxSortMemory());
    assertEquals(4, mrSettings.getMergeFanin());
    assertEquals(10, mrSettings.getMillisPerSlice());
    assertEquals(5, mrSettings.getSortBatchPerEmitBytes());
    assertEquals(6, mrSettings.getSortReadTimeMillis());
    assertEquals("queue1", mrSettings.getWorkerQueueName());

    builder = MapReduceSettings.builder()
      .bucketName("app_default_bucket")
      .module("m1");
    mrSettings = builder.build();
    assertEquals("m1", mrSettings.getModule());
  }
}
