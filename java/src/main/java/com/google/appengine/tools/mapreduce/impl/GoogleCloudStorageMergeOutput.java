package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.*;
import com.google.appengine.tools.mapreduce.impl.sort.LexicographicalComparator;
import com.google.appengine.tools.mapreduce.impl.util.SerializableValue;
import com.google.appengine.tools.mapreduce.outputs.*;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Defines the way data is written out by the merging stage. This consists of multiple GCS files
 * using Level Db format. Each file contains KeyValue pairs ordered by their key (same format as the
 * sort output).
 *
 */
@RequiredArgsConstructor
public class GoogleCloudStorageMergeOutput extends
    Output<KeyValue<ByteBuffer, List<ByteBuffer>>, FilesByShard> {

  private static final long serialVersionUID = 2L;

  @NonNull
  private final String bucket;
  @NonNull
  private final String mrJobId;
  @NonNull
  private final Integer tier;
  private final GoogleCloudStorageFileOutputWriter.Options options;

  private static class OrderSlicingOutputWriter extends
      ItemSegmentingOutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> {

    private static final long serialVersionUID = 2L;
    private static final Marshaller<ByteBuffer> MARSHALLER = Marshallers.getByteBufferMarshaller();
    private final String bucket;
    private final String fileNamePattern;
    private final List<String> fileNames;
    private SerializableValue<ByteBuffer> lastKey;
    private final GoogleCloudStorageFileOutputWriter.Options options;

    /**
     * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
     *        argument for the slice number.
     */
    public OrderSlicingOutputWriter(String bucket, String fileNamePattern, GoogleCloudStorageFileOutputWriter.Options options) {
      this.bucket = checkNotNull(bucket, "Null bucket");
      this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
      this.fileNames = new ArrayList<>();
      this.options = options;
    }

    @Override
    public MarshallingOutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> createNextWriter(
        int sliceNumber) {
      Marshaller<ByteBuffer> identity = Marshallers.getByteBufferMarshaller();
      String fileName = String.format(fileNamePattern, sliceNumber);
      fileNames.add(fileName);
      return new MarshallingOutputWriter<>(
          new GoogleCloudStorageLevelDbOutputWriter(
              new GoogleCloudStorageFileOutputWriter(
                  new GcsFilename(bucket, fileName),
                  MapReduceConstants.REDUCE_INPUT_MIME_TYPE,
                  options.withSupportSliceRetries(false))),
          Marshallers.getKeyValuesMarshaller(identity, identity));
    }

    public List<String> getFilesCreated() {
      return fileNames;
    }

    @Override
    public long estimateMemoryRequirement() {
      return GoogleCloudStorageFileOutputWriter.MEMORY_REQUIRED_WITHOUT_SLICE_RETRY;
    }

    @Override
    protected boolean shouldSegment(KeyValue<ByteBuffer, List<ByteBuffer>> value) {
      boolean result = lastKey != null
          && LexicographicalComparator.compareBuffers(lastKey.getValue(), value.getKey()) > 0;
      lastKey = SerializableValue.of(MARSHALLER, value.getKey());
      return result;
    }
  }

  /**
   * Returns a writer that writes the data the same way that the sort does, splitting the output
   * every time the key goes backwards in sequence. This way the output is a collection of files
   * that are individually fully sorted. This works in conjunction with
   * {@link GoogleCloudStorageMergeInput} to convert a large number of sorted files into a much
   * smaller number of sorted files.
   */
  @Override
  public List<? extends OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> createWriters(
      int shards) {
    ImmutableList.Builder<OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> result =
        new ImmutableList.Builder<>();
    for (int i = 0; i < shards; i++) {
      result.add(new OrderSlicingOutputWriter(bucket,
          String.format(MapReduceConstants.MERGE_OUTPUT_DIR_FORMAT, mrJobId, tier, i), options));
    }
    return result.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public FilesByShard finish(
      Collection<? extends OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> writers) {
    FilesByShard filesByShard = new FilesByShard(writers.size(), bucket);
    int shard = 0;
    for (OutputWriter<?> w : writers) {
      OrderSlicingOutputWriter writer = (OrderSlicingOutputWriter) w;
      filesByShard.addFilesToShard(shard++, writer.getFilesCreated());
    }
    return filesByShard;
  }
}
