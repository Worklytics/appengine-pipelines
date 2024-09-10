package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import com.google.appengine.tools.pipeline.Job;
import com.google.cloud.datastore.DatastoreOptions;

/**
 * A pipeline job to delete persistent data for a sharded job.
 */
public class DeleteShardedJob extends AbstractShardedJob {

  private static final long serialVersionUID = -6850669259843382958L;


  private final DatastoreOptions datastoreOptions;

  public DeleteShardedJob(DatastoreOptions datastoreOptions, String jobId, int taskCount) {
    super(jobId, taskCount);
    this.datastoreOptions = datastoreOptions;
  }

  @Override
  protected Job<?> createShardsJob(int start, int end) {
    return new DeleteShardsInfos(datastoreOptions, getJobId(), start, end);
  }

  @Override
  public String getJobDisplayName() {
    return "DeleteShardedJob: " + getJobId();
  }
}
