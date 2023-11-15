package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.DatastoreExtension;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({DatastoreExtension.class, DatastoreExtension.ParameterResolver.class})
public class AppEngineBackEndTest {

  AppEngineBackEnd appEngineBackEnd;

  Key root = Key.newBuilder("test-project", "JOb", "test-kind", "test-name").build();
  Key generator = Key.newBuilder("test-project", "JOb", "test-kind", "test-name").build();

  @BeforeEach
  void setUp(Datastore datastore) {
    appEngineBackEnd = new AppEngineBackEnd(datastore, new AppEngineTaskQueue());
  }

  @SneakyThrows
  @Test
  void serializeValue_smallValue() {
    Slot slot = new Slot(root, generator, "test-graph-guid", appEngineBackEnd.getSerializationStrategy());

    long[] smallValue = new long[10];
    fill(smallValue);

    Object v = appEngineBackEnd.serializeValue(slot, smallValue);
    assertTrue(v instanceof Blob);

    long[] deserialized = (long[]) appEngineBackEnd.deserializeValue(slot, v);

    assertTrue(deserialized instanceof long[]);
    assertTrue(Arrays.compare(smallValue, deserialized) == 0);
  }


  @SneakyThrows
  @Test
  void serializeValue_largeValue() {
    Slot slot = new Slot(root, generator, "test-graph-guid", appEngineBackEnd.getSerializationStrategy());

    //16 MB --> requires 17 slots in datastore
    long[] largeValue = new long[2000000];
    fill(largeValue);


    Object v = appEngineBackEnd.serializeValue(slot, largeValue);
    assertTrue(v instanceof java.util.List);

    java.util.List<Key> keys = (java.util.List<Key>) v;

    Object deserialized = appEngineBackEnd.deserializeValue(slot, v);
    assertTrue(deserialized instanceof long[]);
  }


  void fill(long[] value) {
    Random random = new Random();
    for (int i = 0; i < value.length; i++) {
      value[i] = random.nextLong();
    }
  }

}
