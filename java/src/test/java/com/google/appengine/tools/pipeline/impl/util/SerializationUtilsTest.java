package com.google.appengine.tools.pipeline.impl.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

class SerializationUtilsTest {


  // range of values, so test with both compressed and not compressed
  // NOTE: original google pipelines version of this had 1e6, 2e6 cases super slow ... 7s, 13s each - wtf; must have
  // been copying arrays rather than streaming or something
  @ParameterizedTest
  @ValueSource(ints = {5, 100, 1000, 10_000, 1_000_000, 2_000_000})
  public void roundtrip(int longs) throws IOException, ClassNotFoundException {

    long[] largeValue = new long[longs];
    for (int i = 0; i < largeValue.length; i++) {
      largeValue[i] = i;
    }
    long[] serializationUtilsTest = (long[]) SerializationUtils.deserialize(SerializationUtils.serialize(largeValue));

    assertArrayEquals(largeValue, serializationUtilsTest);

    assertEquals("hello",
      SerializationUtils.deserialize(SerializationUtils.serialize(new String("hello"))));
  }

}