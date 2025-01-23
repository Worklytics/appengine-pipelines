package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.test.DatastoreExtension;
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

  Key root, generator;


  @BeforeEach
  void setUp(Datastore datastore) {
    appEngineBackEnd = new AppEngineBackEnd(datastore, new AppEngineTaskQueue());

    root = Key.newBuilder(datastore.getOptions().getProjectId(), "JOb", "test-kind", datastore.getOptions().getDatabaseId()).build();
    generator = Key.newBuilder(datastore.getOptions().getProjectId(), "JOb", "test-kind", datastore.getOptions().getDatabaseId()).build();

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

    //int desiredBytes = 16_000_000;  //16 MB --> requires 17 slots in datastore
    //  in emulator at least, this gives error about exceeding framesize ~4MB ... although 10MB is supposedly max txn size
    // [datastore] io.grpc.StatusRuntimeException: INTERNAL: Frame size 16010560 exceeds maximum: 4194304. If this is normal, increase the maxMessageSize in the channel/server builder
    // above *looks* like an emulator on the emulator-side
    int desiredBytes = 4_128_000;  //biggest number that works
    //int desiredBytes = 4_000_000; //4 MB --> requires 5 slots in datastore
    //int desiredBytes = 2_000_000;  //1 MB --> requires 2 slots in datastore
    long[] largeValue = new long[desiredBytes / 8];
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
