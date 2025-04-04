package com.google.appengine.tools.mapreduce.impl.sort;

import static com.google.appengine.tools.mapreduce.CounterNames.MERGE_CALLS;
import static com.google.appengine.tools.mapreduce.CounterNames.MERGE_WALLTIME_MILLIS;
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
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunId;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Passes input to the output. (All the real work is done in those classes)
 *
 */
public class MergeShardTask extends WorkerShardTask<KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
    KeyValue<ByteBuffer, List<ByteBuffer>>, MergeContext> {

  private static final long serialVersionUID = -4974946015047508342L;
  private InputReader<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> in;
  private OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> out;
  private MergeWorker worker;
  private final Integer sortReadTimeMillis; // Only null as a result of an old version.

  public MergeShardTask(ShardedJobRunId mrJobId, int shardNumber, int shardCount,
                        InputReader<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> in,
                        OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> out, int sortReadTimeMillis) {
    super(new IncrementalTaskContext(mrJobId, shardNumber, shardCount, MERGE_CALLS,
        MERGE_WALLTIME_MILLIS), WorkerShardTask.WorkerRunSettings.defaults());
    this.sortReadTimeMillis = sortReadTimeMillis;
    this.in = checkNotNull(in, "Null in");
    this.out = checkNotNull(out, "Null out");
    this.worker = new MergeWorker();
    fillContext();
  }

  /**
   * Passes data from the input to the output.
   */
  private static class MergeWorker extends Worker<MergeContext> {
    private static final long serialVersionUID = -8898621644158681288L;

    public void emit(KeyValue<ByteBuffer, Iterator<ByteBuffer>> input) {
      List<ByteBuffer> values = Lists.newArrayList(input.getValue());
      getContext().emit(new KeyValue<>(input.getKey(), values));
    }
  }

  @Override
  protected void callWorker(KeyValue<ByteBuffer, Iterator<ByteBuffer>> input) {
    worker.emit(input);
  }

  @Override
  protected String formatLastWorkItem(KeyValue<ByteBuffer, Iterator<ByteBuffer>> item) {
    if (item == null) {
      return "null";
    }
    ByteBuffer key = item.getKey().slice();
    key.limit(key.position() + Math.min(MAX_LAST_ITEM_STRING_SIZE, key.remaining()));
    CharBuffer string = UTF_8.decode(key);
    return "Key: " + string + (string.length() >= MAX_LAST_ITEM_STRING_SIZE ? " ..." : "");
  }

  @Override
  protected boolean shouldCheckpoint(long timeElapsed) {
    return timeElapsed
        >= (sortReadTimeMillis == null ? DEFAULT_SORT_READ_TIME_MILLIS : sortReadTimeMillis);
  }

  @Override
  protected long estimateMemoryRequirement() {
    return in.estimateMemoryRequirement() + out.estimateMemoryRequirement();
  }

  @Override
  protected Worker<MergeContext> getWorker() {
    return worker;
  }

  @Override
  public OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> getOutputWriter() {
    return out;
  }

  @Override
  public InputReader<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> getInputReader() {
    return in;
  }

  @Override
  public void jobCompleted(Status status) {
    worker = null;
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
    MergeContext ctx = new MergeContext(getContext(), out);
    in.setContext(ctx);
    out.setContext(ctx);
    worker.setContext(ctx);
  }

  @Override
  public boolean allowSliceRetry(boolean abandon) {
    return out.allowSliceRetry() && worker.allowSliceRetry();
  }
}
