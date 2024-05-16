package com.google.appengine.tools.pipeline.impl.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class SerializationUtilsTest {


  // range of values, so test with both compressed and not compressed
  //TODO: 1e6, 2e6 cases slow ... 7s, 13s each - wtf
  @ParameterizedTest
  @ValueSource(ints = {5, 100, 1000, 10_000, 1_000_000, 2_000_000})
  public void roundtrip(int longs) throws IOException {

    long[] largeValue = new long[longs];
    for (int i = 0; i < largeValue.length; i++) {
      largeValue[i] = i;
    }
    //make sure underlying SerializationUtils works OK this form of it works
    long[] serializationUtilsTest = (long[]) SerializationUtils.deserialize(SerializationUtils.serialize(largeValue));


    assertEquals("hello",
      SerializationUtils.deserialize(SerializationUtils.serialize(new String("hello"))));

  }
}