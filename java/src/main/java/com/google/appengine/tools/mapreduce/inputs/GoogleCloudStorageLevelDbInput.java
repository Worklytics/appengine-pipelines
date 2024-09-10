package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import lombok.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * GoogleCloudStorageLevelDbInput creates LevelDbInputReaders to read input written out by
 * {@link com.google.appengine.tools.mapreduce.outputs.LevelDbOutput} to files in
 * Google Cloud Storage.
 *
 */
@RequiredArgsConstructor
public final class GoogleCloudStorageLevelDbInput extends Input<ByteBuffer> {

  private static final long serialVersionUID = 2L;

  @NonNull
  private final GoogleCloudStorageFileSet files;
  @NonNull
  private final GoogleCloudStorageLineInput.Options options;


  public GoogleCloudStorageLevelDbInput(GoogleCloudStorageFileSet files) {
    this(files, GoogleCloudStorageLineInput.BaseOptions.defaults().withBufferSize(MapReduceConstants.DEFAULT_IO_BUFFER_SIZE));
  }

  /**
   * @param files The set of files to create readers for. One reader per file.
   * @param bufferSize The size of the buffer used for each file.
   */
  public GoogleCloudStorageLevelDbInput(GoogleCloudStorageFileSet files, int bufferSize) {
    this(files, GoogleCloudStorageLineInput.BaseOptions.defaults().withBufferSize(bufferSize));
  }


  @Override
  public List<InputReader<ByteBuffer>> createReaders() {
    List<InputReader<ByteBuffer>> result = new ArrayList<>();
    for (GcsFilename file : files.getFiles()) {
      result.add(new GoogleCloudStorageLevelDbInputReader(file, options));
    }
    return result;
  }
}
