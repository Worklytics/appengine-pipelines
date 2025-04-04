package com.google.appengine.tools.mapreduce.impl.sort;

import static com.google.appengine.tools.mapreduce.CounterNames.SORT_CALLS;
import static com.google.appengine.tools.mapreduce.CounterNames.SORT_WALLTIME_MILLIS;
import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_SORT_READ_TIME_MILLIS;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.MAX_LAST_ITEM_STRING_SIZE;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.RecoverableException;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.List;

/**
 * Keeps accepting inputs and passing them to a SortWorker until the worker's buffer is full. Once
 * this occurs endSlice is called which causes the data to be written out in sorted order.
 *
 */
public class SortShardTask extends WorkerShardTask<KeyValue<ByteBuffer, ByteBuffer>,
    KeyValue<ByteBuffer, List<ByteBuffer>>, SortContext> {

  private static final long serialVersionUID = -8041992113450564646L;

  private SortWorker inMemSorter;
  private InputReader<KeyValue<ByteBuffer, ByteBuffer>> in;
  private OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> out;
  private final Integer sortReadTimeMillis;  // Only null as a result of an old version.

  public SortShardTask(ShardedJobRunId mrJobId, int shardNumber, int shardCount,
                       InputReader<KeyValue<ByteBuffer, ByteBuffer>> in, SortWorker worker,
                       OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> out, int sortReadTimeMillis) {
    super(new IncrementalTaskContext(mrJobId, shardNumber, shardCount, SORT_CALLS,
        SORT_WALLTIME_MILLIS), WorkerShardTask.WorkerRunSettings.defaults());
    this.sortReadTimeMillis = sortReadTimeMillis;
    this.in = checkNotNull(in, "Null in");
    this.out = checkNotNull(out, "Null out");
    this.inMemSorter = worker;
    fillContext();
  }

  @Override
  public void prepare() {
    super.prepare();
    boolean success = false;
    try {
      inMemSorter.prepare();
      success = true;
    } finally {
      if (!success) {
        super.cleanup();
      }
    }
  }

  @Override
  public void cleanup() {
    super.cleanup();
    inMemSorter.cleanup();
  }

  @Override
  protected void callWorker(KeyValue<ByteBuffer, ByteBuffer> input) {
    try {
      inMemSorter.addValue(input.getKey(), input.getValue());
    } catch (Exception ex) {
      throw new RecoverableException("sort worker failure", ex);
    }
  }

  @Override
  protected String formatLastWorkItem(KeyValue<ByteBuffer, ByteBuffer> item) {
    if (item == null) {
      return "null";
    }
    ByteBuffer value = item.getValue().slice();
    value.limit(value.position() + Math.min(MAX_LAST_ITEM_STRING_SIZE, value.remaining()));
    CharBuffer string = UTF_8.decode(value);
    return "Key: " + UTF_8.decode(item.getKey().slice()) + " Value: " + string
        + (item.getValue().remaining() >= MAX_LAST_ITEM_STRING_SIZE ? " ..." : "");
  }

  @Override
  protected boolean shouldCheckpoint(long timeElapsed) {
    int timeLimit =
        (sortReadTimeMillis == null ? DEFAULT_SORT_READ_TIME_MILLIS : sortReadTimeMillis);
    return timeElapsed >= timeLimit || inMemSorter.isFull();
  }

  @Override
  protected long estimateMemoryRequirement() {
    return in.estimateMemoryRequirement() + out.estimateMemoryRequirement() +
        inMemSorter.estimateMemoryRequirement();
  }

  @Override
  protected Worker<SortContext> getWorker() {
    return inMemSorter;
  }

  @Override
  public OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> getOutputWriter() {
    return out;
  }

  @Override
  public InputReader<KeyValue<ByteBuffer, ByteBuffer>> getInputReader() {
    return in;
  }

  @Override
  public boolean allowSliceRetry(boolean abandon) {
    return true;
  }

  @Override
  public void jobCompleted(Status status) {
    inMemSorter = null;
    in = null;
    if (out != null) {
      out.cleanup();
      out = null;
    }
    setFinalized();
  }


  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    if (!wasFinalized()) {
      fillContext();
    }
  }

  private void fillContext() {
    SortContext ctx = new SortContext(getContext(), out);
    in.setContext(ctx);
    out.setContext(ctx);
    inMemSorter.setContext(ctx);
  }
}
