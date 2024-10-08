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
    MapReduceSettings mrSettings = new MapReduceSettings.Builder().build();
    assertEquals(DEFAULT_BASE_URL, mrSettings.getBaseUrl());
    assertEquals("app_default_bucket", mrSettings.getBucketName());
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
    MapReduceSettings.Builder builder = new MapReduceSettings.Builder();
    builder.setModule("m").build();
    builder = builder.setBaseUrl("base-url");
    builder = builder.setBucketName("bucket");
    try {
      builder.setMapFanout(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMapFanout(3);
    try {
      builder.setMaxShardRetries(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMaxShardRetries(1);
    try {
      builder.setMaxSliceRetries(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMaxSliceRetries(0);
    try {
      builder.setMaxSortMemory(-1L);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMaxSortMemory(10L);
    try {
      builder.setMergeFanin(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMergeFanin(4);
    try {
      builder.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMillisPerSlice(10);
    try {
      builder.setSortBatchPerEmitBytes(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setSortBatchPerEmitBytes(5);
    try {
      builder.setSortReadTimeMillis(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setSortReadTimeMillis(6);
    builder = builder.setWorkerQueueName("queue1");


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

    builder = new MapReduceSettings.Builder().setModule("m1");

    mrSettings = builder.build();
    assertEquals("m1", mrSettings.getModule());
  }
}
