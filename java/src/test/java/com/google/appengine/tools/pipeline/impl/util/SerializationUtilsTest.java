package com.google.appengine.tools.pipeline.impl.util;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.*;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class SerializationUtilsTest {

  @Test
  public void helloWorld() throws IOException, ClassNotFoundException {
    assertEquals("hello",
      SerializationUtils.deserialize(SerializationUtils.serialize(new String("hello"))));
  }

  // range of values, so test with both compressed and not compressed
  // NOTE: original google pipelines version of this had 1e6, 2e6 cases super slow ... 7s, 13s each - wtf; must have
  // been copying arrays rather than streaming or something
  // as of 17 May 2024, using java.util.zip.* impl, perf linear - O(n) - in size of array
  // as a perf test, maybe somewhat unrealistic bc *highly* compressible data
  @ParameterizedTest
  @CsvSource({
      // longs == 8 bytes; compression threshold is 50k bytes
      "10,false", // 80 bytes
      "100,false", // 800 bytes
      "1000,false", // 8k
      "10000,true", // 80k
      "49000,true", // 392k
      "50000,true", // 400k
      "100000,true", // 800k
      "150000,true", // 1.2MB
      "1000000,true", // 8MB
      "2000000,true", // 16MB
  })
  public void roundtrip(int longs, boolean compressed) throws IOException, ClassNotFoundException {

    long[] largeValue = generateLongArray(longs);
    byte[] serialized = SerializationUtils.serialize(largeValue);

    // verify expectations for case are correct
    assertEquals(compressed, 8*largeValue.length > SerializationUtils.MAX_UNCOMPRESSED_BYTE_SIZE);

    // verify in fact compressed or not
    assertEquals(compressed, SerializationUtils.isGZIPCompressed(serialized));

    //survives roundtrip
    long[] serializationUtilsTest = (long[]) SerializationUtils.deserialize(serialized);
    assertArrayEquals(largeValue, serializationUtilsTest);
  }


  @SneakyThrows
  @Test
  public void isGZIPCompressed() {
    byte[] testSequence = Arrays.stream(generateLongArray(60_000))
      .mapToObj(l -> Long.valueOf(l).toString())
      .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString().getBytes();

    assertFalse(SerializationUtils.isGZIPCompressed(testSequence));
    assertTrue(SerializationUtils.isGZIPCompressed(SerializationUtils.serialize(testSequence)));
  }

  long[] generateLongArray(int size) {
    long[] array = new long[size];
    for (int i = 0; i < size; i++) {
      array[i] = i;
    }
    return array;
  }

}