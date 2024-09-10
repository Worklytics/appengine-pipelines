package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.MAP_OUTPUT_MIME_TYPE;
import static com.google.common.base.Preconditions.checkNotNull;


import com.google.appengine.tools.mapreduce.*;
import com.google.appengine.tools.mapreduce.outputs.*;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import lombok.NonNull;
import lombok.ToString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An {@link OutputWriter} that is used by the map stage and writes bytes to GCS,
 * later to be used by the sort/merge stages.
 * Content is written in a LevelDb log Format and then using the {@link KeyValueMarshaller}
 * to marshall the individual record.
 *
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 */
public class GoogleCloudStorageMapOutputWriter<K, V>
    extends ShardingOutputWriter<K, V, GoogleCloudStorageMapOutputWriter.MapOutputWriter<K, V>> {

  private static final long serialVersionUID = 739934506831898405L;
  private static final Logger logger =
      Logger.getLogger(GoogleCloudStorageMapOutputWriter.class.getName());

  private final String fileNamePattern;
  private final String bucket;
  private final KeyValueMarshaller<K, V> keyValueMarshaller;
  private final GoogleCloudStorageFileOutput.Options options;


  public GoogleCloudStorageMapOutputWriter(String bucket, String fileNamePattern,
      Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller, Sharder sharder, GoogleCloudStorageFileOutput.Options options) {
    super(keyMarshaller, sharder);
    this.bucket =  checkNotNull(bucket, "Null bucket");
    this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
    keyValueMarshaller = new KeyValueMarshaller<>(keyMarshaller, valueMarshaller);
    this.options = options;
  }

  @Override
  public boolean allowSliceRetry() {
    return true;
  }

  @Override
  public MapOutputWriter<K, V> createWriter(int sortShard) {
    String namePrefix = String.format(fileNamePattern, sortShard);
    return new MapOutputWriter<>(new GcsFileOutputWriter(bucket, namePrefix, this.options), keyValueMarshaller);
  }

  @Override
  protected Map<Integer, MapOutputWriter<K, V>> getShardsToWriterMap() {
    return super.getShardsToWriterMap();
  }

  @Override
  public long estimateMemoryRequirement() {
    return sharder.getNumShards() * GcsFileOutputWriter.MEMORY_REQUIRED;
  }

  static class MapOutputWriter<K, V> extends MarshallingOutputWriter<KeyValue<K, V>> {

    private static final long serialVersionUID = 6056683766896574858L;
    private final GcsFileOutputWriter gcsWriter;

    public MapOutputWriter(GcsFileOutputWriter gcsWriter, KeyValueMarshaller<K, V> marshaller) {
      super(new LevelDbOutputWriter(gcsWriter), marshaller);
      this.gcsWriter = gcsWriter;
    }

    Iterable<String> getFiles() {
      return gcsWriter.getFiles();
    }
  }

  @ToString
  private static class GcsFileOutputWriter extends OutputWriter<ByteBuffer> {

    private static final long serialVersionUID = 2L;
    private static final String MAX_COMPONENTS_PER_COMPOSE = "com.google.appengine.tools.mapreduce"
        + ".impl.GoogleCloudStorageMapOutputWriter.MAX_COMPONENTS_PER_COMPOSE";
    private static final String MAX_FILES_PER_COMPOSE = "com.google.appengine.tools.mapreduce.impl"
        + ".GoogleCloudStorageMapOutputWriter.MAX_FILES_PER_COMPOSE";
    @ToString.Exclude
    private transient Storage client;

    private static final long MEMORY_REQUIRED = MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 2;


    private final int maxComponentsPerCompose;
    private final int maxFilesPerCompose;
    private final String bucket;
    private final String namePrefix;
    private final Set<String> toDelete = new HashSet<>();
    private final Set<String> sliceParts = new LinkedHashSet();
    private final Set<String> compositeParts = new LinkedHashSet<>();
    @NonNull
    private final GoogleCloudStorageFileOutput.Options options;
    private String filePrefix;
    private int fileCount;

    @ToString.Exclude
    private transient WriteChannel channel;
    @ToString.Exclude
    private transient Blob sliceBlob;

    public GcsFileOutputWriter(String bucket, String namePrefix, GoogleCloudStorageFileOutput.Options options) {
      this.bucket = bucket;
      this.namePrefix = namePrefix;
      int temp = Integer.parseInt(System.getProperty(MAX_COMPONENTS_PER_COMPOSE, "1024"));
      maxFilesPerCompose = Integer.parseInt(System.getProperty(MAX_FILES_PER_COMPOSE, "32"));
      maxComponentsPerCompose = (temp / maxFilesPerCompose) * maxFilesPerCompose;
      Preconditions.checkArgument(maxFilesPerCompose > 0);
      Preconditions.checkArgument(maxComponentsPerCompose > 0);
      this.options = options;
    }


    protected Storage getClient() {
      if (client == null) {
        //TODO: set retry param (GCS_RETRY_PARAMETERS)
        //TODO: set User-Agent to "App Engine MR"?
        client = GcpCredentialOptions.getStorageClient(this.options);
      }
      return client;
    }

    @Override
    public void cleanup() {
      for (String name : toDelete) {
        try {
          getClient().delete(BlobId.of(bucket, name));
        } catch (StorageException ex) {
          logger.log(Level.WARNING, "Could not cleanup temporary file " + name, ex);
        }
      }
      toDelete.clear();
    }

    @Override
    public void beginShard() {
      toDelete.addAll(sliceParts);
      toDelete.addAll(compositeParts);
      cleanup();
      sliceParts.clear();
      compositeParts.clear();
      fileCount = 0;
      channel = null;
      filePrefix = namePrefix + "-" + new Random().nextLong();
    }

    @Override
    public void beginSlice() throws IOException {
      cleanup();
      if (sliceParts.size() >= maxFilesPerCompose) {
        String tempFile = generateTempFileName();
        compose(sliceParts, tempFile);
        compositeParts.add(tempFile);
      }
      if (compositeParts.size() * maxFilesPerCompose >= maxComponentsPerCompose) {
        compose(compositeParts, getFileName(fileCount++));
      }
    }

    @Override
    public void write(ByteBuffer bytes) throws IOException {
      if (channel == null) {
        sliceBlob = getClient().create(
          BlobInfo.newBuilder(bucket, generateTempFileName())
            .setContentType(MAP_OUTPUT_MIME_TYPE).build());
        channel = sliceBlob.writer();
        channel.setChunkSize(DEFAULT_IO_BUFFER_SIZE);
      }
      while (bytes.hasRemaining()) {
        channel.write(bytes);
      }
    }

    @Override
    public void endSlice() throws IOException {
      if (channel != null) {
        channel.close();
        sliceParts.add(sliceBlob.getBlobId().getName());
        channel = null;
      }
    }

    @Override
    public void endShard() throws IOException {
      if (!sliceParts.isEmpty()) {
        String tempFile = generateTempFileName();
        compose(sliceParts, tempFile);
        compositeParts.add(tempFile);
      }
      if (!compositeParts.isEmpty()) {
        compose(compositeParts, getFileName(fileCount++));
      }
    }

    private String generateTempFileName() {
      while (true) {
        String tempFileName = filePrefix + "-" + new Random().nextLong();
        if (!sliceParts.contains(tempFileName) && !compositeParts.contains(tempFileName)) {
          return tempFileName;
        }
      }
    }

    private String getFileName(int part) {
      return filePrefix + "@" + part;
    }

    private void compose(Collection<String> source, String target) throws IOException {
      BlobInfo targetFile = BlobInfo.newBuilder(bucket, target).build();
      if (source.size() == 1) {
        getClient().copy(Storage.CopyRequest.of(BlobId.of(bucket, source.iterator().next()), targetFile));
      } else {
        getClient().compose(Storage.ComposeRequest.of(source, targetFile));
      }
      for (String name : source) {
        if (!name.equals(target)) {
          toDelete.add(name);
        }
      }
      source.clear();
    }

    private Iterable<String> getFiles() {
      return new Iterable<String>() {
        @Override public Iterator<String> iterator() {
          return new AbstractIterator<String>() {
            private int index = 0;

            @Override
            protected String computeNext() {
              if (index < fileCount) {
                return getFileName(index++);
              }
              return endOfData();
            }
          };
        }
      };
    }
  }
}
