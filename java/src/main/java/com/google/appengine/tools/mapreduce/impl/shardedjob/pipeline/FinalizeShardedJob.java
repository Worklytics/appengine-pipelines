package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.pipeline.Job;
import com.google.cloud.datastore.DatastoreOptions;

/**
 * A pipeline job for finalizing the job and cleaning up unnecessary state.
 */
public class FinalizeShardedJob extends AbstractShardedJob {

  private static final long serialVersionUID = -6850669259843382958L;
  private final Status status;
  private final DatastoreOptions datastoreOptions;

  public FinalizeShardedJob(DatastoreOptions datastoreOptions, String jobId, int taskCount, Status status) {
    super(jobId, taskCount);
    this.status = status;
    this.datastoreOptions = datastoreOptions;
  }

  @Override
  protected Job<?> createShardsJob(int start, int end) {
    return new FinalizeShardsInfos(this.datastoreOptions, getJobId(), status, start, end);
  }

  @Override
  public String getJobDisplayName() {
    return "FinalizeShardedJob: " + getJobId();
  }
}
