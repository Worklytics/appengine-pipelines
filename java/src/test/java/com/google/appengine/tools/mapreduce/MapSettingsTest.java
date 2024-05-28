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

import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.OnBackend;
import com.google.appengine.tools.pipeline.JobSetting.OnService;
import com.google.appengine.tools.pipeline.JobSetting.OnQueue;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Environment;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import org.easymock.EasyMock;
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
@SuppressWarnings("deprecation")
public class MapSettingsTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig(),
      new LocalModulesServiceTestConfig()
        .addBasicScalingModuleVersion("module1", "v1", 10)
        .addBasicScalingModuleVersion("module1", "v2", 10)
        .addBasicScalingModuleVersion("default", "1", 1)
        .addBasicScalingModuleVersion("default", "2", 1));

  Datastore datastore;

  @BeforeEach
  public void setUp(Datastore datastore) {
    helper.setUp();
    Map<String, Object> attributes = ApiProxy.getCurrentEnvironment().getAttributes();
    @SuppressWarnings("unchecked")
    Map<String, Object> portMap =
        (Map<String, Object>) attributes.get("com.google.appengine.devappserver.portmapping");
    if (portMap == null) {
      portMap = new HashMap<>();
      attributes.put("com.google.appengine.devappserver.portmapping", portMap);
    }
    portMap.put("b1", "backend-hostname");
    this.datastore = datastore;
  }

  @Test
  public void testDefaultSettings() {
    MapSettings mrSettings = new MapSettings.Builder().build();
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
    MapSettings.Builder builder = new MapSettings.Builder();

    builder.setModule("m").build();

    builder.setWorkerQueueName("queue1");
    builder.setBaseUrl("base-url");
    builder.setMillisPerSlice(10);
    try {
      builder.setMillisPerSlice(-1);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder.setSliceTimeoutRatio(1.5);
    try {
      builder.setSliceTimeoutRatio(0.8);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException ex) {
      //expected
    }
    builder.setMaxShardRetries(1);
    try {
      builder.setMaxShardRetries(-1);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder.setMaxSliceRetries(0);
    try {
      builder.setMaxSliceRetries(-1);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    MapSettings settings = builder.build();
    assertEquals("queue1", settings.getWorkerQueueName());
    assertEquals("base-url", settings.getBaseUrl());
    assertEquals(10, settings.getMillisPerSlice());
    assertEquals(1, settings.getMaxShardRetries());
    assertEquals(0, settings.getMaxSliceRetries());
    builder.setModule("m1");
    settings = builder.build();
    assertEquals("m1", settings.getModule());
  }

  @Test
  public void testValidate() throws Exception {
    MapSettings.Builder builder = new MapSettings.Builder();
    // TODO(user): replace "bad_queue" with "bad-queue". The latter is just
    // an invalid name and does not check if queue exists. see b/13910616
    builder.setWorkerQueueName("bad_queue");
    try {
      builder.build();
      fail("was expecting failure due to bad queue");
    } catch (RuntimeException ex) {
      // expected.
    }
  }

  @Test
  public void testBuilderWithSettings() {
    MapSettings settings = new MapSettings.Builder()
        .setModule("m")
        .setBaseUrl("url")
        .setMaxShardRetries(10)
        .setMaxSliceRetries(20)
        .setMillisPerSlice(30)
        .setWorkerQueueName("good-queue")
        .build();
    settings = new MapSettings.Builder(settings).build();
    assertEquals("m", settings.getModule());
    assertEquals("url", settings.getBaseUrl());
    assertEquals(10, settings.getMaxShardRetries());
    assertEquals(20, settings.getMaxSliceRetries());
    assertEquals(30, settings.getMillisPerSlice());
    assertEquals("good-queue", settings.getWorkerQueueName());
  }

  @Test
  public void testMakeShardedJobSettings() {
    Key key = datastore.newKeyFactory().setKind("Kind1").newKey("value1");
    MapSettings settings = new MapSettings.Builder().setWorkerQueueName("good-queue").build();
    ShardedJobSettings sjSettings = settings.toShardedJobSettings("job1", key);
    assertEquals("default", sjSettings.getModule());
    assertEquals("1", sjSettings.getVersion());
    assertEquals("1.default.test.localhost", sjSettings.getTaskQueueTarget());
    assertEquals(settings.getWorkerQueueName(), sjSettings.getQueueName());
    assertEquals(getPath(settings, "job1", CONTROLLER_PATH), sjSettings.getControllerPath());
    assertEquals(getPath(settings, "job1", WORKER_PATH), sjSettings.getWorkerPath());
    assertEquals(makeViewerUrl(key, key), sjSettings.getPipelineStatusUrl());
    assertEquals(settings.getMaxShardRetries(), sjSettings.getMaxShardRetries());
    assertEquals(settings.getMaxSliceRetries(), sjSettings.getMaxSliceRetries());


    settings = new MapSettings.Builder(settings).setModule("module1").build();
    sjSettings = settings.toShardedJobSettings("job1", key);
    assertEquals("v1.module1.test.localhost", sjSettings.getTaskQueueTarget());
    assertEquals("module1", sjSettings.getModule());
    assertEquals("v1", sjSettings.getVersion());

    settings = new MapSettings.Builder(settings).setModule("default").build();
    Environment env = ApiProxy.getCurrentEnvironment();
    Environment mockEnv = EasyMock.createNiceMock(Environment.class);
    EasyMock.expect(mockEnv.getModuleId()).andReturn("default").atLeastOnce();
    EasyMock.expect(mockEnv.getVersionId()).andReturn("2").atLeastOnce();
    EasyMock.expect(mockEnv.getAttributes()).andReturn(env.getAttributes()).anyTimes();
    EasyMock.replay(mockEnv);
    ApiProxy.setEnvironmentForCurrentThread(mockEnv);
    // Test when current module is the same as requested module
    try {
      sjSettings = settings.toShardedJobSettings("job1", key);
      assertEquals("default", sjSettings.getModule());
      assertEquals("2", sjSettings.getVersion());
    } finally {
      ApiProxy.setEnvironmentForCurrentThread(env);
    }
    EasyMock.verify(mockEnv);
  }

  private String getPath(MapSettings settings, String jobId, String logicPath) {
    return settings.getBaseUrl() + logicPath + "/" + jobId;
  }

  @Test
  public void testPipelineSettings() {
    MapSettings mrSettings = new MapSettings.Builder().setWorkerQueueName("queue1").build();
    verifyPipelineSettings(mrSettings.toJobSettings(), new ServiceValidator(null), new QueueValidator("queue1"));

    mrSettings = new MapSettings.Builder().setModule("m1").build();
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
