package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for {@code PeekingInputReader}.
 */
public class PeekingInputReaderTest {

  private static final Marshaller<Long> MARSHALLER = Marshallers.getLongMarshaller();

  @SuppressWarnings("serial")
  private static class MarshallingInputReader<T> extends InputReader<ByteBuffer> {
    private final InputReader<T> input;
    private final Marshaller<T> marshaller;

    MarshallingInputReader(InputReader<T> input, Marshaller<T> marshaller) {
      this.input = input;
      this.marshaller = marshaller;
    }

    @Override
    public ByteBuffer next() throws IOException, NoSuchElementException {
      return marshaller.toBytes(input.next());
    }

    @Override
    public Double getProgress() {
      return input.getProgress();
    }
  }

  @Test
  public void testPeeking() throws IOException {
    int numRecords = 10;
    PeekingInputReader<Long> reader = new PeekingInputReader<>(new MarshallingInputReader<>(
        new ConsecutiveLongInput.Reader(0L, numRecords), MARSHALLER), MARSHALLER);
    for (Long i = 0L; i < numRecords; i++) {
      assertEquals(i, reader.peek());
      assertEquals(i, reader.next());
    }
    assertNull(reader.peek());
    assertThrowsNoSuchElement(reader);
  }

  @Test
  public void testPeekingTwice() throws IOException {
    int numRecords = 10;
    PeekingInputReader<Long> reader = new PeekingInputReader<>(new MarshallingInputReader<>(
        new ConsecutiveLongInput.Reader(0L, numRecords), MARSHALLER), MARSHALLER);
    for (Long i = 0L; i < numRecords; i++) {
      assertEquals(i, reader.peek());
      assertEquals(i, reader.peek());
      assertEquals(i, reader.next());
    }
    assertNull(reader.peek());
    assertThrowsNoSuchElement(reader);
  }

  @Test
  public void testNotPeeking() throws IOException {
    int numRecords = 10;
    PeekingInputReader<Long> reader = new PeekingInputReader<>(new MarshallingInputReader<>(
        new ConsecutiveLongInput.Reader(0L, numRecords), MARSHALLER), MARSHALLER);
    for (Long i = 0L; i < numRecords; i++) {
      assertEquals(i, reader.next());
    }
    assertThrowsNoSuchElement(reader);
  }

  @Test
  public void testSerializeWithoutPeeking() throws IOException {
    int numRecords = 10;
    PeekingInputReader<Long> reader = new PeekingInputReader<>(new MarshallingInputReader<>(
        new ConsecutiveLongInput.Reader(0L, numRecords), MARSHALLER), MARSHALLER);
    for (Long i = 0L; i <  numRecords; i++) {
      reader = SerializationUtil.clone(reader);
      assertEquals(i, reader.next());
    }
    reader = SerializationUtil.clone(reader);
    assertThrowsNoSuchElement(reader);
  }

  /**
   * Tests Peeking after Reconstruct with nothing peeked.
   */
  @Test
  public void testPeekingAfterSerialization() throws IOException {
    int numRecords = 10;
    PeekingInputReader<Long> reader = new PeekingInputReader<>(new MarshallingInputReader<>(
        new ConsecutiveLongInput.Reader(0L, numRecords), MARSHALLER), MARSHALLER);
    for (Long i = 0L; i < numRecords; i++) {
      reader = SerializationUtil.clone(reader);
      assertEquals(i, reader.peek());
      assertEquals(i, reader.next());
    }
    reader = SerializationUtil.clone(reader);
    assertNull(reader.peek());
    assertThrowsNoSuchElement(reader);
  }

  /**
   * Tests Next after Reconstruct with nothing peeked.
   */
  @Test
  public void testNextAfterSerialization() throws IOException {
    int numRecords = 10;
    PeekingInputReader<Long> reader = new PeekingInputReader<>(new MarshallingInputReader<>(
        new ConsecutiveLongInput.Reader(0L, numRecords), MARSHALLER), MARSHALLER);
    for (Long i = 0L; i < numRecords; i++) {
      reader = SerializationUtil.clone(reader);
      assertEquals(i, reader.next());
    }
    reader = SerializationUtil.clone(reader);
    assertNull(reader.peek());
    assertThrowsNoSuchElement(reader);
  }

  /**
   * Tests the following cases:
   * Peek after Reconstruct with something peeked.
   * Next after Reconstruct with something peeked.
   */
  @Test
  public void testPeekingWithSerialization() throws IOException {
    int numRecords = 10;
    PeekingInputReader<Long> reader = new PeekingInputReader<>(new MarshallingInputReader<>(
        new ConsecutiveLongInput.Reader(0L, numRecords), MARSHALLER), MARSHALLER);
    for (Long i = 0L; i < numRecords; i++) {
      assertEquals(i, reader.peek());
      reader = SerializationUtil.clone(reader);
      assertEquals(i, reader.peek());
      reader = SerializationUtil.clone(reader);
      assertEquals(i, reader.next());
    }
    assertNull(reader.peek());
    reader = SerializationUtil.clone(reader);
    assertThrowsNoSuchElement(reader);
  }

  private void assertThrowsNoSuchElement(InputReader<Long> in) throws IOException {
    try {
      in.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }
}
