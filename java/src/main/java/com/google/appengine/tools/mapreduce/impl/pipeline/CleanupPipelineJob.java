package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.GcpCredentialOptions;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.pipeline.*;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * A pipeline to delete MR result with a FilesByShard and removing its traces when completed
 * (therefore should be called as a new pipeline via the {@link #cleanup} method).
 */
@RequiredArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class CleanupPipelineJob extends Job1<Void, List<GcsFilename>> {

  private static final long serialVersionUID = -5473046989460252781L;
  private static final int DELETE_BATCH_SIZE = 100;
  private static final long DELETION_DELAY = 10_000L;

  private final GcpCredentialOptions gcpCredentialOptions;

  @Override
  public Value<Void> run(List<GcsFilename> files) {
    List<List<GcsFilename>> batches = Lists.partition(files, DELETE_BATCH_SIZE);
    int index = 0;
    @SuppressWarnings("unchecked")
    FutureValue<Void>[] futures = new FutureValue[batches.size()];
    for (List<GcsFilename> batch : batches) {
      FutureValue<Void> futureCall =
          futureCall(new DeleteFilesJob(gcpCredentialOptions), immediate(new ArrayList<>(batch)));
      futures[index++] = futureCall;
    }
    return Jobs.waitForAllAndDeleteWithDelay(this, DELETION_DELAY, null, futures);
  }

  public static void cleanup(PipelineService pipelineService,
                             GcpCredentialOptions gcpCredentialOptions,
                             List<GcsFilename> toDelete,
                             JobSetting... settings) {
    pipelineService.startNewPipeline(new CleanupPipelineJob(gcpCredentialOptions), toDelete, settings);
  }
}
