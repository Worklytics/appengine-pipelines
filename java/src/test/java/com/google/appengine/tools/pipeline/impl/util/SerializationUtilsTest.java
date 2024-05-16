package com.google.appengine.tools.pipeline.impl.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class SerializationUtilsTest {


  @Test
  public void roundtrip() throws IOException {

    //2M longs, not random, should be OK
    long[] largeValue = new long[2000000];
    for (int i = 0; i < largeValue.length; i++) {
      largeValue[i] = i;
    }
    //make sure underlying SerializationUtils works OK this form of it works
    long[] serializationUtilsTest = (long[]) SerializationUtils.deserialize(SerializationUtils.serialize(largeValue));


    assertEquals("hello",
      SerializationUtils.deserialize(SerializationUtils.serialize(new String("hello"))));

  }
}