package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.pipeline.util.CloseUtils;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.Serial;
import java.nio.channels.ReadableByteChannel;

@RequiredArgsConstructor
/**
 * A simple wrapper of LevelDb wrapper for GCS to provide getProgress() and do lazy initialization.
 */
public final class GoogleCloudStorageLevelDbInputReader extends LevelDbInputReader {

  @Serial
  private static final long serialVersionUID = 2L;

  @NonNull
  private final GcsFilename file;
  @NonNull
  private final GoogleCloudStorageLineInputReader.Options options;

  // The length of the file being read; -1 if unknown.
  private long length = -1;

  private transient volatile Storage client;


  /**
   * @param file File to be read.
   * @param bufferSize The buffersize to be used by the Gcs prefetching read channel.
   */
  public GoogleCloudStorageLevelDbInputReader(GcsFilename file, int bufferSize) {
    this(file, GoogleCloudStorageLineInput.BaseOptions.defaults().withBufferSize(bufferSize));
  }

  protected Storage getClient() throws IOException {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          //TODO: set retry param (GCS_RETRY_PARAMETERS)
          //TODO: set User-Agent to "App Engine MR"?
          client = StorageOptions.getDefaultInstance().getService();
        }
      }
    }
    return client;
  }

  @Override
  public void beginShard() throws IOException {
    super.beginShard();
    length = -1;
  }

  @Override
  public Double getProgress() {
    try (Storage client = getClient()) {
      if (length == -1) {
        Blob blob = null;
        try {
          blob = client.get(file.asBlobId());
        } catch (StorageException e) {
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
    } catch (Throwable ignored) {
      // closing - presumably
    }
    return null;
  }

  @Override
  public void endSlice() throws IOException {
    CloseUtils.close(getClient());
    super.endSlice();
  }

  @Override
  public ReadableByteChannel createReadableByteChannel() throws IOException {
    ReadChannel reader = getClient().reader(file.asBlobId());
    reader.setChunkSize(options.getBufferSize());
    return reader;
  }

  @Override
  public long estimateMemoryRequirement() {
    return LevelDbConstants.BLOCK_SIZE + options.getBufferSize() * 2; // Double buffered
  }
}
