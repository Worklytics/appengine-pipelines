// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_MILLIS_PER_SLICE;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_TIMEOUT_RATIO;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SHARD_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.WORKER_PATH;
import static com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet.makeViewerUrl;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.OnService;
import com.google.appengine.tools.pipeline.JobSetting.OnQueue;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.appengine.tools.pipeline.PipelineService;

import com.google.cloud.datastore.Datastore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
@PipelineSetupExtensions
public class MapSettingsTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig());

  Datastore datastore;
  PipelineService pipelineService;

  @BeforeEach
  public void setUp(Datastore datastore) {
    helper.setUp();
    this.datastore = datastore;
  }

  @BeforeEach
  public void mockPipelineService() {
    this.pipelineService = mock(PipelineService.class);
    when(pipelineService.getDefaultWorkerService()).thenReturn("default");
    when(pipelineService.getCurrentVersion(eq("default"))).thenReturn("1");
    when(pipelineService.getCurrentVersion(eq("module1"))).thenReturn("v1");
    when(pipelineService.getCurrentVersion(eq("module2"))).thenReturn("v2");
  }

  @Test
  public void testDefaultSettings() {
    MapSettings mrSettings = MapSettings.builder().build();
    assertNull(mrSettings.getModule());
    assertNull(mrSettings.getWorkerQueueName());
    assertEquals(DEFAULT_BASE_URL, mrSettings.getBaseUrl());
    assertEquals(DEFAULT_MILLIS_PER_SLICE, mrSettings.getMillisPerSlice());
    assertEquals(DEFAULT_SHARD_RETRIES, mrSettings.getMaxShardRetries());
    assertEquals(DEFAULT_SLICE_RETRIES, mrSettings.getMaxSliceRetries());
    assertEquals(DEFAULT_SLICE_TIMEOUT_RATIO, mrSettings.getSliceTimeoutRatio());
  }

  @Test
  public void testNonDefaultSettings() {
    MapSettings.MapSettingsBuilder builder = MapSettings.builder();

    builder.module("m").build();

    builder.workerQueueName("queue1");
    builder.baseUrl("base-url");
    builder.millisPerSlice(10);
    builder.sliceTimeoutRatio(1.5);
    try {
      builder.sliceTimeoutRatio(0.8);
      fail("Expected IllegalArgumentException to be thrown sliceTimeoutRatio must be > 1");
    } catch (IllegalArgumentException ex) {
      //expected
    }
    builder.maxShardRetries(1);
    try {
      builder.maxShardRetries(-1);
      fail("Expected IllegalArgumentException to be thrown maxShardRetries must be >= 0");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder.maxSliceRetries(0);
    try {
      builder.maxSliceRetries(-1);
      fail("Expected IllegalArgumentException to be thrown maxSliceRetries must be >= 0");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    MapSettings settings = builder.build();
    assertEquals("queue1", settings.getWorkerQueueName());
    assertEquals("base-url", settings.getBaseUrl());
    assertEquals(10, settings.getMillisPerSlice());
    assertEquals(1, settings.getMaxShardRetries());
    assertEquals(0, settings.getMaxSliceRetries());
    builder.module("m1");
    settings = builder.build();
    assertEquals("m1", settings.getModule());
  }

  @Test
  public void testBuilderWithSettings() {
    MapSettings settings = MapSettings.builder()
        .module("m")
        .baseUrl("url")
        .maxShardRetries(10)
        .maxSliceRetries(20)
        .millisPerSlice(30)
        .workerQueueName("good-queue")
        .build();
    settings = settings.toBuilder().build();
    assertEquals("m", settings.getModule());
    assertEquals("url", settings.getBaseUrl());
    assertEquals(10, settings.getMaxShardRetries());
    assertEquals(20, settings.getMaxSliceRetries());
    assertEquals(30, settings.getMillisPerSlice());
    assertEquals("good-queue", settings.getWorkerQueueName());
  }

  @Test
  public void testMakeShardedJobSettings() {
    JobRunId pipelineRunId = JobRunId.of(datastore.getOptions().getProjectId(),
      datastore.getOptions().getDatabaseId(),
      datastore.getOptions().getNamespace(), "pipeline");

    MapSettings settings = MapSettings.builder().workerQueueName("good-queue").build();
    ShardedJobRunId shardedJobId = ShardedJobRunId.of(datastore.getOptions().getProjectId(),
      datastore.getOptions().getDatabaseId(),
      datastore.getOptions().getNamespace(),  "job1");
    ShardedJobSettings sjSettings = ShardedJobSettings.from(pipelineService, settings, shardedJobId, pipelineRunId);
    assertEquals("default", sjSettings.getModule());
    assertEquals("1", sjSettings.getVersion());
    assertEquals(settings.getWorkerQueueName(), sjSettings.getQueueName());
    assertEquals(getPath(settings, shardedJobId.asEncodedString(), CONTROLLER_PATH), sjSettings.getControllerPath());
    assertEquals(getPath(settings, shardedJobId.asEncodedString(), WORKER_PATH), sjSettings.getWorkerPath());
    assertEquals(makeViewerUrl(pipelineRunId, shardedJobId), sjSettings.getPipelineStatusUrl());
    assertEquals(settings.getMaxShardRetries(), sjSettings.getMaxShardRetries());
    assertEquals(settings.getMaxSliceRetries(), sjSettings.getMaxSliceRetries());


    settings = settings.toBuilder().module("module1").build();
    sjSettings = ShardedJobSettings.from(pipelineService, settings, shardedJobId, pipelineRunId);
    assertEquals("module1", sjSettings.getModule());
    assertEquals("v1", sjSettings.getVersion());


    settings = settings.toBuilder().module("default").build();

    when(pipelineService.getDefaultWorkerService()).thenReturn("default");
    when(pipelineService.getCurrentVersion(eq("default"))).thenReturn("2");
    when(pipelineService.getCurrentVersion(eq("module1"))).thenReturn("v1");
    when(pipelineService.getCurrentVersion(eq("module2"))).thenReturn("v2");

      sjSettings = ShardedJobSettings.from(pipelineService, settings, shardedJobId, pipelineRunId);
      assertEquals("default", sjSettings.getModule());
      assertEquals("2", sjSettings.getVersion());

    verify(pipelineService, atLeastOnce()).getDefaultWorkerService();
    verify(pipelineService, atLeastOnce()).getCurrentVersion(eq("default"));
  }

  private String getPath(MapSettings settings, String jobId, String logicPath) {
    return settings.getBaseUrl() + logicPath + "/" + jobId;
  }

  @Test
  public void testPipelineSettings() {
    MapSettings mrSettings = MapSettings.builder().workerQueueName("queue1").build();
    verifyPipelineSettings(mrSettings.toJobSettings(), new ServiceValidator(null), new QueueValidator("queue1"));

    mrSettings =MapSettings.builder().module("m1").build();
    verifyPipelineSettings(mrSettings.toJobSettings(new StatusConsoleUrl("u1")), new ServiceValidator("m1"),
        new QueueValidator(null), new StatusConsoleValidator("u1"));
  }

  @SafeVarargs
  final void verifyPipelineSettings(
      JobSetting[] settings, Validator<? extends JobSetting, ?>... validators) {
    Map<Class<? extends JobSetting>, Validator<? extends JobSetting, ?>> expected = new HashMap<>();
    for (Validator<? extends JobSetting, ?> v : validators) {
      expected.put(v.getType(), v);
    }
    Set<Class<? extends JobSetting>> unique = new HashSet<>();
    for (JobSetting setting : settings) {
      Class<? extends JobSetting> settingClass = setting.getClass();
      unique.add(settingClass);
      if (expected.containsKey(settingClass)) {
        expected.get(settingClass).validate(setting);
      } else {
        // no validator for setting, don't really care atm
        //ail("No validator for setting: " + settingClass);
      }
    }
    //assertEquals(expected.size(), unique.size());
  }

  private abstract class Validator<T extends JobSetting, V> {

    private final V expected;

    Validator(V value) {
      expected = value;
    }

    @SuppressWarnings("unchecked")
    void validate(JobSetting value) {
      assertEquals(expected, getValue((T) value));
    }

    @SuppressWarnings("unchecked")
    Class<T> getType() {
      return (Class<T>)
          ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    protected abstract V getValue(T value);
  }

  private class ServiceValidator extends Validator<OnService, String> {

    ServiceValidator(String value) {
      super(value);
    }

    @Override
    protected String getValue(OnService value) {
      return value.getValue();
    }
  }

  private class QueueValidator extends Validator<OnQueue, String> {

    QueueValidator(String value) {
      super(value);
    }

    @Override
    protected String getValue(OnQueue value) {
      return value.getValue();
    }
  }

  private class StatusConsoleValidator extends Validator<StatusConsoleUrl, String> {

    StatusConsoleValidator(String value) {
      super(value);
    }

    @Override
    protected String getValue(StatusConsoleUrl value) {
      return value.getValue();
    }
  }
}
