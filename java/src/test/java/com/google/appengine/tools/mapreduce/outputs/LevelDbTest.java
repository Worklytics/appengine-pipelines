package com.google.appengine.tools.mapreduce.outputs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.mapreduce.inputs.LevelDbInputReader;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.function.Supplier;

/**
 * Tests for {@link LevelDbInputReader} and {@link LevelDbOutputWriter}
 */
public class LevelDbTest {

  private static final int BLOCK_SIZE = LevelDbConstants.BLOCK_SIZE;
  private static final int MAX_SINGLE_BLOCK_RECORD_SIZE =
      LevelDbConstants.BLOCK_SIZE - LevelDbConstants.HEADER_LENGTH;



  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings("serial")
  private static class TestLevelDbInputReader extends LevelDbInputReader {

    private final Supplier<ReadableByteChannel> channelSupplier;

    TestLevelDbInputReader(Supplier<ReadableByteChannel>  channelSupplier, int blockSize) {
      super(blockSize);
      this.channelSupplier = channelSupplier;
    }

    @Override
    public ReadableByteChannel createReadableByteChannel() {
      return channelSupplier.get();
    }
  }

  /**
   * Writes to an in memory byte array for testing.
   *
   */
  static class ByteArrayOutputWriter extends OutputWriter<ByteBuffer> {

    private static final long serialVersionUID = -6345005368809269034L;
    ByteArrayOutputStream bout = new ByteArrayOutputStream();

    @Override
    public void write(ByteBuffer value) throws IOException {
      bout.write(SerializationUtils.getBytes(value));
    }

    @Override
    public void endShard() throws IOException {
      bout.close();
    }

    public byte[] toByteArray() {
      return bout.toByteArray();
    }
  }

  @Test
  public void testDataCorruption() throws IOException {
    Random r = new Random(0);
    int overriddenBlockSize = 100;
    ByteArrayOutputWriter arrayOutputWriter = new ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new LevelDbOutputWriter(arrayOutputWriter, overriddenBlockSize);
    writer.beginShard();
    List<byte[]> written = writeRandomItems(r, writer, 11, 10); // Bigger than one block
    writer.endShard();
    byte[] writtenData = arrayOutputWriter.toByteArray();

    for (int i = 0; i < writtenData.length; i++) {
      for (int j = 0; j < 8; j++) {
        writtenData[i] = (byte) (writtenData[i] ^ (1 << j));
        LevelDbInputReader reader =
            new TestLevelDbInputReader(() -> Channels.newChannel(new ByteArrayInputStream(writtenData)), overriddenBlockSize);
        try {
          verifyWrittenData(written, reader);
          fail();
        } catch (CorruptDataException e) {
          // Expected
        }
        writtenData[i] = (byte) (writtenData[i] ^ (1 << j));
      }
    }
  }

  /**
   * This really does not test much since levelDb explicitly tolerates truncation at record
   * boundaries. So all it really validates is that we don't throw some weird error.
   */
  @Test
  public void testDataTruncation() throws IOException {
    Random r = new Random(0);
    int overriddenBlockSize = 100;
    ByteArrayOutputWriter arrayOutputWriter = new ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new LevelDbOutputWriter(arrayOutputWriter, overriddenBlockSize);
    writer.beginShard();
    List<byte[]> written = writeRandomItems(r, writer, 11, 10); // Bigger than one block
    writer.endShard();
    byte[] writtenData = arrayOutputWriter.toByteArray();
    for (int i = 0; i < 11 * 10; i++) {
      byte[] toRead = Arrays.copyOf(writtenData, i);
      LevelDbInputReader reader =
          new TestLevelDbInputReader(() -> Channels.newChannel(new ByteArrayInputStream(toRead)), overriddenBlockSize);
      try {
        verifyWrittenData(written, reader);
        fail();
      } catch (CorruptDataException | NoSuchElementException e) {
        // Expected
      }
    }
  }

  @Test
  public void testZeroSizeItems() throws IOException {
    verifyRandomContentRoundTrips(20000, 0);
  }

  @Test
  public void testSmallItems() throws IOException {
    verifyRandomContentRoundTrips(2000, 1000);
  }


  @Test
  public void testSlicingOneBlockPad() throws IOException {
    ByteArrayOutputWriter arrayOutputWriter = new ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new LevelDbOutputWriter(arrayOutputWriter);
    testSlicing(writer, arrayOutputWriter, 10, 100);
  }


  static void testSlicing(LevelDbOutputWriter writer, ByteArrayOutputWriter arrayOutputWriter,
      int num, int size) throws IOException {
    Random r = new Random(0);
    writer.beginShard();
    List<byte[]> written = writeRandomItems(r, writer, num, size);
    written.addAll(writeRandomItems(r, writer, num, size));
    writer.endShard();
    byte[] writtenData = arrayOutputWriter.toByteArray();
    assertEquals(0, writtenData.length % BLOCK_SIZE);
    LevelDbInputReader reader = new TestLevelDbInputReader(() -> Channels.newChannel(new ByteArrayInputStream(writtenData)));
    verifyWrittenData(written, reader);
  }

  @Test
  public void testNearBlockSizeItems() throws IOException {
    for (int i = 0; i <= 64; i++) {
      verifyRandomContentRoundTrips(5, BLOCK_SIZE - i);
    }
  }


  @Test
  public void testBlockAlignedItems() throws IOException {
    int number = 100;
    int size = MAX_SINGLE_BLOCK_RECORD_SIZE;
    verifyRandomContentRoundTrips(number, size);
  }

  @Test
  public void testLargerThanBlockItems() throws IOException {
    int number = 100;
    int size = MAX_SINGLE_BLOCK_RECORD_SIZE + 1;
    verifyRandomContentRoundTrips(number, size);
  }


  @Test
  public void testSmallerThanBlockItems() throws IOException {
    int number = 100;
    int size = MAX_SINGLE_BLOCK_RECORD_SIZE - 1;
    verifyRandomContentRoundTrips(number, size);
  }

  @Test
  public void testMultiBlockAlignedItems() throws IOException {
    int number = 2;
    int size = MAX_SINGLE_BLOCK_RECORD_SIZE * 64;
    verifyRandomContentRoundTrips(number, size);
  }

  @Test
  public void testLargerThanMultiBlockItems() throws IOException {
    verifyRandomContentRoundTrips(1, BLOCK_SIZE * 64 - 1);
    for (int i = 1; i < 10; i++) {
      verifyRandomContentRoundTrips(1, MAX_SINGLE_BLOCK_RECORD_SIZE * 3 - i);
      verifyRandomContentRoundTrips(1, MAX_SINGLE_BLOCK_RECORD_SIZE * 3 + i);
    }
  }

  private void verifyRandomContentRoundTrips(int number, int size) throws IOException {
    Random r = new Random(0);
    ByteArrayOutputWriter arrayOutputWriter = new ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new LevelDbOutputWriter(arrayOutputWriter);
    writer.beginShard();
    List<byte[]> written = writeRandomItems(r, writer, number, size);
    writer.endShard();
    byte[] writtenData = arrayOutputWriter.toByteArray();
    assertEquals(0, writtenData.length % BLOCK_SIZE);
    LevelDbInputReader reader = new TestLevelDbInputReader(() -> Channels.newChannel(new ByteArrayInputStream(writtenData)));
    verifyWrittenData(written, reader);
  }

  static void verifyWrittenData(List<byte[]> written, LevelDbInputReader reader)
      throws IOException {
    reader.beginShard();
    reader.beginSlice();
    for (int i = 0; i < written.size(); i++) {
      ByteBuffer read = reader.next();
      assertArrayEquals(written.get(i), SerializationUtils.getBytes(read));
    }
    try {
      reader.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
    reader.endSlice();
    reader.endShard();
  }

  static List<byte[]> writeRandomItems(Random r, LevelDbOutputWriter writer, int number, int size)
      throws IOException {
    writer.beginSlice();
    List<byte[]> written = new ArrayList<>();
    for (int i = 0; i < number; i++) {
      byte[] data = new byte[size];
      r.nextBytes(data);
      written.add(data);
      writer.write(ByteBuffer.wrap(data));
    }
    writer.endSlice();
    return written;
  }

}
