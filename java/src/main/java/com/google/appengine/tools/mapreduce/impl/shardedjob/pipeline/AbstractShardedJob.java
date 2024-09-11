package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobId;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.Jobs;
import com.google.appengine.tools.pipeline.Value;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A base class for a sharded-job pipeline.
 */
@RequiredArgsConstructor
public abstract class AbstractShardedJob extends Job0<Void> {

  private static final long serialVersionUID = 6498588928999409114L;
  private static final int SHARDS_PER_JOB = 20;
  private static final JobSetting[] CHILD_JOB_PARAMS = {};
  private static final long DELETION_DELAY = 10_000L;

  @Getter
  private final ShardedJobId jobId;
  private final int taskCount;

  @Override
  public Value<Void> run() {
    int childJobs = (int) Math.ceil(taskCount / (double) SHARDS_PER_JOB);
    FutureValue<?>[] waitFor = new FutureValue[childJobs];
    int startOffset = 0;
    for (int i = 0; i < childJobs; i++) {
      int endOffset = Math.min(taskCount, startOffset + SHARDS_PER_JOB);
      waitFor[i] = futureCallUnchecked(
          getChildJobParams(), createShardsJob(startOffset, endOffset));
      startOffset = endOffset;
    }
    return Jobs.waitForAllAndDeleteWithDelay(this, DELETION_DELAY, null, waitFor);
  }

  protected abstract Job<?> createShardsJob(int start, int end);

  protected JobSetting[] getChildJobParams() {
    return CHILD_JOB_PARAMS;
  }
}
