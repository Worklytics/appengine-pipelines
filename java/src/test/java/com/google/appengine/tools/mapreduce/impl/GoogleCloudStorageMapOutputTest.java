// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.*;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLineInput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test class for {@link GoogleCloudStorageMapOutput}.
 *
 */
public class GoogleCloudStorageMapOutputTest {

  private static final ShardedJobRunId JOB = ShardedJobRunId.of("test-project", null, null,"JOB1");
  private static final Marshaller<Long> KEY_MARSHALLER = Marshallers.getLongMarshaller();
  private static final Marshaller<String> VALUE_MARSHALLER = Marshallers.getStringMarshaller();
  private static final int FILES_PER_COMPOSE = 32;
  private static final int COMPONENTS_PER_COMPOSE = FILES_PER_COMPOSE * 2;
  private static final String COMPONENTS_PER_COMPOSE_PROPERTY = "com.google.appengine.tools"
      + ".mapreduce.impl.GoogleCloudStorageMapOutputWriter.MAX_COMPONENTS_PER_COMPOSE";
  private static final Random RND = new SecureRandom();

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();
  private final CloudStorageIntegrationTestHelper cloudStorageIntegrationTestHelper = new CloudStorageIntegrationTestHelper();

  @BeforeEach
  public void setUp() {
    helper.setUp();
    System.setProperty(COMPONENTS_PER_COMPOSE_PROPERTY, String.valueOf(COMPONENTS_PER_COMPOSE));
    cloudStorageIntegrationTestHelper.setUp();
  }

  @AfterEach
  public void tearDown() {
    helper.tearDown();
    System.clearProperty(COMPONENTS_PER_COMPOSE_PROPERTY);
    cloudStorageIntegrationTestHelper.tearDown();
  }

  @Test
  public void testNoContent() throws IOException {
    writeAndVerifyContent(SliceData.of(0, 0, 0));
  }
  @Test
  public void testSingleFilePartNoIntermediateCompositeParts() throws IOException {
    writeAndVerifyContent(SliceData.of(1, 100, 100));
    writeAndVerifyContent(SliceData.of(FILES_PER_COMPOSE - 1, 100, 100));
  }

  @Test
  public void testSingleFilePartWithIntermediateCompositeParts() throws IOException {
    writeAndVerifyContent(SliceData.of(FILES_PER_COMPOSE, 100, 100));
    writeAndVerifyContent(SliceData.of(FILES_PER_COMPOSE + 1, 100, 100));
    writeAndVerifyContent(SliceData.of(FILES_PER_COMPOSE, 100, 100),
        SliceData.of(FILES_PER_COMPOSE, 0, 0));
    writeAndVerifyContent(SliceData.of(COMPONENTS_PER_COMPOSE -  1, 100, 100));
  }

  @Test
  public void testWithMultipleFileParts() throws IOException  {
    writeAndVerifyContent(SliceData.of(COMPONENTS_PER_COMPOSE, 100, 100));
    writeAndVerifyContent(SliceData.of(COMPONENTS_PER_COMPOSE + 1, 100, 100));
    writeAndVerifyContent(SliceData.of(COMPONENTS_PER_COMPOSE * 3, 100, 100));
  }

  private List<KeyValue<Long, String>> createRandomValues(int maxValues, int maxValueSize) {
    if (maxValues == 0) {
      return Collections.emptyList();
    }
    int count = RND.nextInt(maxValues);
    List<KeyValue<Long, String>> values = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      int length = RND.nextInt(maxValueSize);
      StringBuilder value = new StringBuilder(length);
      for (int j = 0; j < length; j++) {
        value.append(RND.nextInt(10));
      }
      values.add(new KeyValue<>(RND.nextLong(), value.toString()));
    }
    return values;
  }

  @AllArgsConstructor(staticName = "of")
  private static class SliceData {

    private final int slices;
    private final int maxValuesPerSlice;
    private final int maxValueSize;
  }

  private void writeAndVerifyContent(SliceData... sliceData) throws IOException  {
    GoogleCloudStorageFileOutput.BaseOptions outputOptions = GoogleCloudStorageFileOutput.BaseOptions.builder()
      .serviceAccountKey(cloudStorageIntegrationTestHelper.getBase64EncodedServiceAccountKey())
      .projectId(cloudStorageIntegrationTestHelper.getProjectId()).build();
    GoogleCloudStorageLineInput.BaseOptions inputOptions = GoogleCloudStorageLineInput.BaseOptions.builder()
      .serviceAccountKey(cloudStorageIntegrationTestHelper.getBase64EncodedServiceAccountKey())
      .build();

    GoogleCloudStorageMapOutput<Long, String> output = new GoogleCloudStorageMapOutput<>(cloudStorageIntegrationTestHelper.getBucket(),
        JOB, KEY_MARSHALLER, VALUE_MARSHALLER, new Sharder() {
          private static final long serialVersionUID = 1L;

          @Override
          public int getNumShards() {
            return 1;
          }

          @Override
          public int getShardForKey(ByteBuffer key) {
            return 0;
          }
        }, outputOptions);
    List<? extends OutputWriter<KeyValue<Long, String>>> writers = output.createWriters(1);
    assertEquals(1, writers.size());
    List<KeyValue<Long, String>> values = new ArrayList<>();
    OutputWriter<KeyValue<Long, String>> writer = writers.get(0);
    int sliceCount = 0;
    writer.beginShard();
    for (SliceData data : sliceData) {
      for (int i = 0; i < data.slices; i++) {
        writer.beginSlice();
        List<KeyValue<Long, String>> randomValues =
            createRandomValues(data.maxValuesPerSlice, data.maxValueSize);
        for (KeyValue<Long, String> value : randomValues) {
          writer.write(value);
          values.add(value);
        }
        if (!randomValues.isEmpty()) {
          sliceCount++;
        }
        writer.endSlice();
      }
    }
    writer.endShard();
    FilesByShard filesByShard = output.finish(writers);
    assertEquals(1, filesByShard.getShardCount());
    List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> input =
        new GoogleCloudStorageSortInput(filesByShard, inputOptions).createReaders();
    assertEquals(1, input.size());
    int expectedFiles = (int) Math.ceil((double) sliceCount / COMPONENTS_PER_COMPOSE);
    assertEquals(expectedFiles, filesByShard.getFilesForShard(0).getNumFiles());
    Iterator<KeyValue<Long, String>> shardValues = values.iterator();
    InputReader<KeyValue<ByteBuffer, ByteBuffer>> reader = input.get(0);
    reader.beginShard();
    reader.beginSlice();
    try {
      while (true) {
        KeyValue<ByteBuffer, ByteBuffer> kv = reader.next();
        KeyValue<Long, String> expected = shardValues.next();
        KeyValue<Long, String> value = new KeyValue<>(KEY_MARSHALLER.fromBytes(kv.getKey()),
            VALUE_MARSHALLER.fromBytes(kv.getValue()));
        assertEquals(expected, value);
      }
    } catch (NoSuchElementException expected) {
      // reader has no more data.
    }
    reader.endSlice();
    reader.endShard();
    assertFalse(shardValues.hasNext());
  }
}
