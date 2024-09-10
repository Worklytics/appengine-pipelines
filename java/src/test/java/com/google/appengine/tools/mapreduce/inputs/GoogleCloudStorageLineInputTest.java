package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.InputReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for {@link GoogleCloudStorageLineInput}.
 */
public class GoogleCloudStorageLineInputTest extends GoogleCloudStorageLineInputTestCase {

  private static final String FILENAME = "CloudStorageLineInputTestFile";
  public static final String RECORD = "01234567890\n";
  public static final int RECORDS_COUNT = 1000;

  GcsFilename filename;
  long fileSize;
  GoogleCloudStorageLineInput.BaseOptions inputOptions;

  @BeforeEach
  public void setUpFile() throws Exception {
    filename = new GcsFilename(cloudStorageIntegrationTestHelper.getBucket(), FILENAME);
    fileSize = createFile(filename.getObjectName(), RECORD, RECORDS_COUNT);
    inputOptions = GoogleCloudStorageLineInput.BaseOptions.defaults().withServiceAccountKey(cloudStorageIntegrationTestHelper.getBase64EncodedServiceAccountKey());
  }

  @Test
  public void testSplit() throws Exception {
    GoogleCloudStorageLineInput input = new GoogleCloudStorageLineInput(filename, (byte) '\n', 4, inputOptions);
    List<? extends InputReader<byte[]>> readers = input.createReaders();
    assertEquals(4, readers.size());
    assertSplitRange(0, 3000, readers.get(0));
    assertSplitRange(3000, 6000, readers.get(1));
    assertSplitRange(6000, 9000, readers.get(2));
    assertSplitRange(9000, 12000, readers.get(3));
  }

  @Test
  public void testUnevenSplit() throws Exception {
    GoogleCloudStorageLineInput input = new GoogleCloudStorageLineInput(filename, (byte) '\n', 7, inputOptions);
    List<? extends InputReader<byte[]>> readers = input.createReaders();
    assertEquals(7, readers.size());
    assertSplitRange(0, 1714, readers.get(0));
    assertSplitRange(1714, 3428, readers.get(1));
    assertSplitRange(3428, 5142, readers.get(2));
    assertSplitRange(5142, 6857, readers.get(3));
    assertSplitRange(6857, 8571, readers.get(4));
    assertSplitRange(8571, 10285, readers.get(5));
    assertSplitRange(10285, 12000, readers.get(6));
  }

  private static void assertSplitRange(int start, int end, InputReader<byte[]> reader) {
    GoogleCloudStorageLineInputReader r = (GoogleCloudStorageLineInputReader) reader;
    assertEquals(start, r.startOffset, "Start offset mismatch");
    assertEquals( end, r.endOffset, "End offset mismatch");
  }
}
