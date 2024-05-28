// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInput;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLineInput;
import com.google.appengine.tools.mapreduce.inputs.PeekingInputReader;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Defines the way the data is read in by the reducer. This consists of a number of files in GCS
 * where the content is sorted {@link KeyValue} of K and a list of V written LevelDb format.
 * {@link KeyValuesMarshaller} to unmarshall the individual records. To maintain the sorted order a
 * {@link MergingReader} is used.
 *
 *
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 */
@RequiredArgsConstructor
public class GoogleCloudStorageReduceInput<K, V> extends Input<KeyValue<K, Iterator<V>>> {

  private static final long serialVersionUID = 2L;

  @NonNull
  private final FilesByShard filesByShard;
  @NonNull
  private final Marshaller<K> keyMarshaller;
  @NonNull
  private final Marshaller<V> valueMarshaller;
  private final GoogleCloudStorageLineInput.Options options;

  @Override
  public List<? extends InputReader<KeyValue<K, Iterator<V>>>> createReaders() {
    Marshaller<KeyValue<ByteBuffer, ? extends Iterable<V>>> marshaller =
        Marshallers.getKeyValuesMarshaller(Marshallers.getByteBufferMarshaller(), valueMarshaller);
    ImmutableList.Builder<MergingReader<K, V>> result = ImmutableList.builder();
    for (int shard = 0; shard < filesByShard.getShardCount(); shard++) {
      result.add(createReaderForShard(marshaller, filesByShard.getFilesForShard(shard)));
    }
    return result.build();
  }

  /**
   * Create a {@link MergingReader} that combines all the input files the reducer to provide a
   * global sort over all data for the shard.
   *
   *  (There are multiple input files in the event that the data didn't fit into the sorter's
   * memory)
   *
   * A {@link MergingReader} is used to combine contents while maintaining key-order. This requires
   * a {@link PeekingInputReader}s to preview the next item of input.
   *
   * @returns a reader producing key-sorted input for a shard.
   */
  private MergingReader<K, V> createReaderForShard(
      Marshaller<KeyValue<ByteBuffer, ? extends Iterable<V>>> marshaller,
      GoogleCloudStorageFileSet reducerInputFileSet) {
    ArrayList<PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>>> inputFiles =
        new ArrayList<>();
    GoogleCloudStorageLevelDbInput reducerInput =
        new GoogleCloudStorageLevelDbInput(reducerInputFileSet, options);
    for (InputReader<ByteBuffer> in : reducerInput.createReaders()) {
      inputFiles.add(new PeekingInputReader<>(in, marshaller));
    }
    return new MergingReader<>(inputFiles, keyMarshaller, true);
  }
}
