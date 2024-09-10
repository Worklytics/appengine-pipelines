// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.*;
import com.google.appengine.tools.mapreduce.inputs.*;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the way the data is read in by the Sort phase. This consists of logically concatenating
 * multiple GoogleCloudStorage files to form a single input of KeyValue pairs. The sorter does not
 * care what the individual values are so they are not deserialized.
 *
 */
@RequiredArgsConstructor
public class GoogleCloudStorageSortInput extends Input<KeyValue<ByteBuffer, ByteBuffer>> {

  private static final long serialVersionUID = 1L;

  @NonNull
  private final FilesByShard files;
  @NonNull
  private final GoogleCloudStorageLineInput.Options options;

  private static class ReaderImpl extends ForwardingInputReader<KeyValue<ByteBuffer, ByteBuffer>> {

    private static final long serialVersionUID = 3310058647644865812L;

    private final InputReader<KeyValue<ByteBuffer, ByteBuffer>> reader;

    private ReaderImpl(GcsFilename file, GoogleCloudStorageLineInput.Options options) {
      Marshaller<ByteBuffer> identity = Marshallers.getByteBufferMarshaller();
      Marshaller<KeyValue<ByteBuffer, ByteBuffer>> marshaller =
          new KeyValueMarshaller<>(identity, identity);
      GoogleCloudStorageLevelDbInputReader in = new GoogleCloudStorageLevelDbInputReader(file, options);
      reader = new UnmarshallingInputReader<>(in, marshaller);
    }

    @Override
    protected InputReader<KeyValue<ByteBuffer, ByteBuffer>> getDelegate() {
      return reader;
    }
  }

  public GoogleCloudStorageSortInput(FilesByShard files) {
    this(files, GoogleCloudStorageLineInput.BaseOptions.defaults());
  }

  @Override
  public List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> createReaders() {
    ImmutableList.Builder<InputReader<KeyValue<ByteBuffer, ByteBuffer>>> out =
        ImmutableList.builder();
    for (int shard = 0; shard < files.getShardCount(); shard++) {
      List<ReaderImpl> readersForShard = new ArrayList<>();
      for (GcsFilename file : files.getFilesForShard(shard).getFiles()) {
        readersForShard.add(new ReaderImpl(file, options));
      }
      out.add(new ConcatenatingInputReader<>(readersForShard));
    }
    return out.build();
  }
}
