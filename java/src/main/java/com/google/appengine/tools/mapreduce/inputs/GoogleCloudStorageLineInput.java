package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.GcpCredentialOptions;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.auth.Credentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.With;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * CloudStorageLineInput shards files in Cloud Storage on separator boundaries.
 */
@RequiredArgsConstructor
public class GoogleCloudStorageLineInput extends Input<byte[]> {

  private static final long MIN_SHARD_SIZE = 1024L;

  private static final long serialVersionUID = 2L;

  private final GcsFilename file;
  private final byte separator;
  private final int shardCount;
  private final Options options;

  /**
   * Options - everything the reader needs, plus anything needed by input at top-level
   */
  public interface Options extends GoogleCloudStorageLineInputReader.Options {

    int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  }

  @Getter
  @With
  @Builder
  public static class BaseOptions implements GoogleCloudStorageLineInput.Options {

    private static final long serialVersionUID = 1L;

    @Builder.Default
    Integer bufferSize = DEFAULT_BUFFER_SIZE;

    private String serviceAccountKey;

    public static BaseOptions defaults() {
      return BaseOptions.builder().build();
    }
  }

  public GoogleCloudStorageLineInput(GcsFilename file, byte separator, int shardCount) {
    this(file, separator, shardCount, Options.DEFAULT_BUFFER_SIZE);
  }

  public GoogleCloudStorageLineInput(
      GcsFilename file, byte separator, int shardCount, int bufferSize) {
    this.file = checkNotNull(file, "Null file");
    this.separator = separator;
    this.shardCount = shardCount;
    this.options = BaseOptions.defaults().withBufferSize(bufferSize);
  }

  @Override
  public List<? extends InputReader<byte[]>> createReaders() throws IOException {
    Storage client = GcpCredentialOptions.getStorageClient(this.options);
    try {
      Blob blob = client.get(file.asBlobId());
      if (blob == null) {
        throw new RuntimeException("File does not exist: " + file);
      }
      return split(file, blob.getSize(), shardCount);
    } catch (StorageException e) {
      throw new RuntimeException("Unable to read file metadata: " + file, e);
    }
  }


  private List<? extends InputReader<byte[]>> split(GcsFilename file,
      long blobSize, int shardCount) {
    Preconditions.checkNotNull(file);
    Preconditions.checkArgument(shardCount > 0);
    Preconditions.checkArgument(blobSize >= 0);

    // Sanity check
    if (shardCount * MIN_SHARD_SIZE > blobSize) {
      shardCount = (int) (blobSize / MIN_SHARD_SIZE) + 1;
    }

    List<GoogleCloudStorageLineInputReader> result = new ArrayList<>();
    long startOffset = 0L;
    for (int i = 1; i < shardCount; i++) {
      long endOffset = (i * blobSize) / shardCount;
      result.add(new GoogleCloudStorageLineInputReader(file, startOffset, endOffset, separator, this.options));
      startOffset = endOffset;
    }
    result.add(new GoogleCloudStorageLineInputReader(file, startOffset, blobSize, separator));
    return result;
  }
}
