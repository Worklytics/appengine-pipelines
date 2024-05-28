package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.GcpCredentialOptions;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

@RequiredArgsConstructor
/**
 * A simple wrapper of LevelDb wrapper for GCS to provide getProgress() and do lazy initialization.
 */
public final class GoogleCloudStorageLevelDbInputReader extends LevelDbInputReader {

  private static final long serialVersionUID = 2L;

  @NonNull
  private final GcsFilename file;
  @NonNull
  private final GoogleCloudStorageLineInputReader.Options options;
  private long length = -1;

  private transient Storage client;


  /**
   * @param file File to be read.
   * @param bufferSize The buffersize to be used by the Gcs prefetching read channel.
   */
  public GoogleCloudStorageLevelDbInputReader(GcsFilename file, int bufferSize) {
    this(file, GoogleCloudStorageLineInput.BaseOptions.defaults().withBufferSize(bufferSize));
  }

  protected Storage getClient() throws IOException {
    if (client == null) {
      //TODO: set retry param (GCS_RETRY_PARAMETERS)
      //TODO: set User-Agent to "App Engine MR"?
      client = GcpCredentialOptions.getStorageClient(this.options);
    }
    return client;
  }

  @Override
  public Double getProgress() {
    if (length == -1) {
      Blob blob = null;
      try {
        blob = getClient().get(file.asBlobId());
      } catch (StorageException | IOException e) {
        // It is just an estimate so it's probably not worth throwing.
      }
      if (blob == null) {
        return null;
      }
      length = blob.getSize();
    }
    if (length == 0f) {
      return null;
    }
    return getBytesRead() / (double) length;
  }

  @Override
  public ReadableByteChannel createReadableByteChannel() throws IOException {
    length = -1;
    ReadChannel reader = getClient().reader(file.asBlobId());
    reader.setChunkSize(options.getBufferSize());
    return reader;
  }

  @Override
  public long estimateMemoryRequirement() {
    return LevelDbConstants.BLOCK_SIZE + options.getBufferSize() * 2; // Double buffered
  }
}
