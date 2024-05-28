package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 */
public class GoogleCloudStorageLineInputReaderTest extends GoogleCloudStorageLineInputTestCase {

  private static final String FILENAME = "GoogleCloudStorageLineInputReaderTestFile";
  public static final String RECORD = "01234567890\n";
  public static final int RECORDS_COUNT = 10;

  GcsFilename filename;
  long fileSize;
  GoogleCloudStorageLineInput.BaseOptions inputOptions;

  @BeforeEach
  public void prepareFile() throws Exception {
    filename = new GcsFilename(cloudStorageIntegrationTestHelper.getBucket(), FILENAME);
    fileSize = createFile(filename.getObjectName(), RECORD, RECORDS_COUNT);
    inputOptions = GoogleCloudStorageLineInput.BaseOptions.defaults().withServiceAccountKey(cloudStorageIntegrationTestHelper.getBase64EncodedServiceAccountKey());
  }

  @Test

  public void testSingleSplitPoint() throws Exception {
    List<GoogleCloudStorageLineInputReader> readers = new ArrayList<>();
    readers.add(new GoogleCloudStorageLineInputReader(filename, 0, RECORD.length(), (byte) '\n', inputOptions));
    readers.add(
        new GoogleCloudStorageLineInputReader(filename, RECORD.length(), fileSize, (byte) '\n', inputOptions));
    verifyReaders(readers, false);
  }

  @Test
  public void testSingleSplitPointsWithSerialization() throws Exception {
    List<GoogleCloudStorageLineInputReader> readers = new ArrayList<>();
    readers.add(new GoogleCloudStorageLineInputReader(filename, 0, RECORD.length(), (byte) '\n', inputOptions));
    readers.add(
        new GoogleCloudStorageLineInputReader(filename, RECORD.length(), fileSize, (byte) '\n', inputOptions));
    verifyReaders(readers, true);
  }

  @Test
  public void testAllSplitPoints() throws Exception {
    for (int splitPoint = 1; splitPoint < fileSize - 1; splitPoint++) {
      List<GoogleCloudStorageLineInputReader> readers = new ArrayList<>();
      readers.add(new GoogleCloudStorageLineInputReader(filename, 0, splitPoint, (byte) '\n', inputOptions));
      readers.add(
          new GoogleCloudStorageLineInputReader(filename, splitPoint, fileSize, (byte) '\n', inputOptions));
      verifyReaders(readers, false);
    }
  }

  @Test
  public void testAllSplitPointsWithSerialization() throws Exception {
    for (int splitPoint = 1; splitPoint < fileSize - 1; splitPoint++) {
      List<GoogleCloudStorageLineInputReader> readers = new ArrayList<>();
      readers.add(new GoogleCloudStorageLineInputReader(filename, 0, splitPoint, (byte) '\n', inputOptions));
      readers.add(
          new GoogleCloudStorageLineInputReader(filename, splitPoint, fileSize, (byte) '\n', inputOptions));
      verifyReaders(readers, true);
    }
  }


  private void verifyReaders(
      List<GoogleCloudStorageLineInputReader> readers, boolean performSerialization)
      throws IOException {
    int recordsRead = 0;
    String recordWithoutSeparator = RECORD.substring(0, RECORD.length() - 1);

    for (GoogleCloudStorageLineInputReader reader : readers) {
      if (performSerialization) {
        reader = SerializationUtil.clone(reader);
      }
      reader.beginShard();
      if (performSerialization) {
        reader = SerializationUtil.clone(reader);
      }
      while (true) {
        reader.beginSlice();
        byte[] value;
        try {
          value = reader.next();
        } catch (NoSuchElementException e) {
          break;
        }
        assertEquals("Record mismatch", recordWithoutSeparator, new String(value));
        recordsRead++;

        reader.endSlice();
        if (performSerialization) {
          reader = SerializationUtil.clone(reader);
        }
      }
      reader.endShard();
    }

    assertEquals(RECORDS_COUNT, recordsRead, "Number of records read");
  }
}
