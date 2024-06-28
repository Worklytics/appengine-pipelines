package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.*;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that writes bytes to a set of Cloud Storage files, one per shard.
 * Produces a single file output (usually on a per-shard basis).
 * This format does not insert any separator characters, so it by default
 * cannot be read back with the CloudStorageLineInputReader.
 *
 */
@RequiredArgsConstructor
public class GoogleCloudStorageFileOutput extends Output<ByteBuffer, GoogleCloudStorageFileSet> {
  private static final long serialVersionUID = 5544139634754912546L;

  @NonNull
  private final String bucket;
  @NonNull
  private final String fileNamePattern;
  @NonNull
  private final String mimeType;
  @NonNull
  private final Options options;


  /**
   * options accepted by this output (superset of those to Writer)
   *
   * functions much likes Beam's PipelineOptions, although not intended to be "global" to Pipeline. For good reason,
   * different instantations of GCS outputs may get different options
   */
  public interface Options extends GoogleCloudStorageFileOutputWriter.Options {

  }

  /**
   * Creates output files who's names follow the provided pattern in the specified bucket.
   * This will construct an instance that supports slice retries.
   *
   * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
   *        argument for the shard number.
   * @param mimeType The string to be passed as the mimeType to GCS.
   */
  @Deprecated
  public GoogleCloudStorageFileOutput(String bucket, String fileNamePattern, String mimeType) {
    this(bucket, fileNamePattern, mimeType, BaseOptions.defaults());
  }

  /**
   * Creates output files who's names follow the provided pattern in the specified bucket.
   *
   * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
   *        argument for the shard number.
   * @param mimeType The string to be passed as the mimeType to GCS.
   * @param supportSliceRetries indicates if slice retries should be supported by this writer.
   *        Slice retries are achieved by writing each slice to a temporary file
   *        and copying it to its destination when processing the next slice.
   */
  @Deprecated
  public GoogleCloudStorageFileOutput(String bucket, String fileNamePattern, String mimeType,
      boolean supportSliceRetries) {
    this(bucket, fileNamePattern, mimeType, BaseOptions.defaults().withSupportSliceRetries(supportSliceRetries));
  }

  @Override
  public List<GoogleCloudStorageFileOutputWriter> createWriters(int numShards) {
    ImmutableList.Builder<GoogleCloudStorageFileOutputWriter> out = ImmutableList.builder();
    for (int i = 0; i < numShards; i++) {
      GcsFilename file = new GcsFilename(bucket, String.format(fileNamePattern, i));
      out.add(new GoogleCloudStorageFileOutputWriter(file, mimeType, options));
    }
    return out.build();
  }

  /**
   * Returns a list of GcsFilename that has one element for each reduce shard.
   */
  @Override
  public GoogleCloudStorageFileSet finish(Collection<? extends OutputWriter<ByteBuffer>> writers) {
    List<String> out = Lists.newArrayList();
    for (OutputWriter<ByteBuffer> w : writers) {
      GoogleCloudStorageFileOutputWriter writer = (GoogleCloudStorageFileOutputWriter) w;
      out.add(writer.getFile().getObjectName());
    }
    return new GoogleCloudStorageFileSet(bucket, out);
  }

  @Getter
  @Builder
  @With
  @ToString
  public static class BaseOptions implements GoogleCloudStorageFileOutput.Options {

    @Builder.Default
    private final Boolean supportSliceRetries = true;

    @Nullable
    private String projectId;

    @Nullable
    private String serviceAccountKey;

    public static BaseOptions defaults() {
      return BaseOptions.builder().build();
    }
  }
}
