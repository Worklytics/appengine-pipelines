package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.CloudStorageIntegrationTestHelper;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.test.PipelineSetupExtensions;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link GoogleCloudStorageLevelDbInput}
 */
@PipelineSetupExtensions
public class GoogleCloudStorageLevelDbInputReaderTest {

  private static final int BLOCK_SIZE = LevelDbConstants.BLOCK_SIZE;
  GcsFilename filename;

  private CloudStorageIntegrationTestHelper storageHelper;

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig());



  @BeforeEach
  public void setUp() throws Exception {
    helper.setUp();
    storageHelper = new CloudStorageIntegrationTestHelper();
    storageHelper.setUp();
    filename = new GcsFilename(storageHelper.getBucket(), "GoogleCloudStorageLevelDbInputReaderTest");
  }

  @AfterEach
  public void tearDown() throws Exception {
    storageHelper.getStorage().delete(filename.asBlobId());
    helper.tearDown();
    storageHelper.tearDown();
  }

  GoogleCloudStorageLineInput.Options inputOptions() {
    return GoogleCloudStorageLineInput.BaseOptions.builder()
      .serviceAccountKey(storageHelper.getBase64EncodedServiceAccountKey())
      .bufferSize(BLOCK_SIZE * 2)
    .build();
  }

  /**
   * generates N random byteBuffers
   * (N also used as seed, so deterministic for a given N)
   */
  private class ByteBufferGenerator implements Iterator<ByteBuffer> {
    Random r;
    int remaining;

    ByteBufferGenerator(int count) {
      this.r = new Random(count);
      this.remaining = count;
    }

    @Override
    public boolean hasNext() {
      return remaining > 0;
    }

    @Override
    public ByteBuffer next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      remaining--;
      int size = r.nextInt(r.nextInt(r.nextInt(BLOCK_SIZE * 4))); // Skew small occasionally large
      byte[] bytes = new byte[size];
      r.nextBytes(bytes);
      return ByteBuffer.wrap(bytes);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  public void writeData(GcsFilename filename, ByteBufferGenerator gen) throws IOException {
    LevelDbOutputWriter writer = new GoogleCloudStorageLevelDbOutputWriter(
        new GoogleCloudStorageFileOutputWriter(filename, "application/leveldb", GoogleCloudStorageFileOutput.BaseOptions.defaults()
          .withServiceAccountKey(storageHelper.getBase64EncodedServiceAccountKey())));
    writer.beginShard();
    writer.beginSlice();
    while (gen.hasNext()) {
      writer.write(gen.next());
    }
    writer.endSlice();
    writer.endShard();
  }

  @Test
  public void testReading() throws IOException {
    final int RANDOM_SEED = 100;
    writeData(filename, new ByteBufferGenerator(RANDOM_SEED));
    GoogleCloudStorageLevelDbInputReader reader =
        new GoogleCloudStorageLevelDbInputReader(filename, inputOptions());
    reader.beginShard();
    ByteBufferGenerator expected = new ByteBufferGenerator(RANDOM_SEED);
    reader.beginSlice();
    while (expected.hasNext()) {
      ByteBuffer read = reader.next();
      assertEquals(expected.next(), read);
    }
    verifyEmpty(reader);
    reader.endSlice();
  }

  @Test
  public void testRecordsDontChange() throws IOException {
    final int RANDOM_SEED = 1000;
    writeData(filename, new ByteBufferGenerator(RANDOM_SEED));
    GoogleCloudStorageLevelDbInputReader reader =
        new GoogleCloudStorageLevelDbInputReader(filename, inputOptions());
    reader.beginShard();
    ByteBufferGenerator expected = new ByteBufferGenerator(RANDOM_SEED);
    reader.beginSlice();
    ArrayList<ByteBuffer> recordsRead = new ArrayList<>();
    try {
      while (true) {
        recordsRead.add(reader.next());
      }
    } catch (NoSuchElementException e) {
      // used a break
    }
    for (int i = 0; i < recordsRead.size(); i++) {
      assertTrue(expected.hasNext());
      ByteBuffer read = recordsRead.get(i);
      assertEquals(expected.next(), read);
    }
    verifyEmpty(reader);
    reader.endSlice();
  }

  /**
   * tests reading with reader undergoing serialization between records
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  // failure case is different in different values ... wtf
  @ValueSource(ints = {17, 34, 100})
  @ParameterizedTest
  public void testReadingWithSerialization(int records) throws IOException, ClassNotFoundException {

    writeData(filename, new ByteBufferGenerator(records));
    GoogleCloudStorageLevelDbInputReader reader =
        new GoogleCloudStorageLevelDbInputReader(filename, inputOptions());
    reader.beginShard();
    ByteBufferGenerator expected = new ByteBufferGenerator(records);
    while (expected.hasNext()) {
      reader = SerializationUtil.clone(reader);
      reader.beginSlice();
      ByteBuffer read = reader.next();
      assertEquals(expected.next(), read);
      reader.endSlice();
    }
    reader = SerializationUtil.clone(reader);
    reader.beginSlice();
    verifyEmpty(reader);
    reader.endSlice();
  }

  private void verifyEmpty(GoogleCloudStorageLevelDbInputReader reader) throws IOException {
    try {
      reader.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  //not a great test, bc block size is a (large) constant
  @CsvSource(value = {
    "blah,0,blah",
    "blah,2,ah",
  })
  @ParameterizedTest
  public void testSkipByOffset(String whole, int skip, String remaining) throws IOException {
    GoogleCloudStorageLevelDbInputReader reader = new GoogleCloudStorageLevelDbInputReader(filename, inputOptions());
    ByteArrayInputStream inputStream = new ByteArrayInputStream(whole.toString().getBytes());
    ReadableByteChannel readChannel = Channels.newChannel(inputStream);
    reader.skipByOffset(readChannel, skip);
    byte[] remainingBytes = new byte[remaining.length()];
    readChannel.read(ByteBuffer.wrap(remainingBytes));
    assertEquals(remaining, new String(remainingBytes));
  }
}
