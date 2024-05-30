// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.impl.shardedjob.*;
import com.google.appengine.tools.pipeline.DefaultDIModule;
import com.google.appengine.tools.pipeline.Injectable;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.pipeline.impl.util.DIUtil;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import javax.inject.Inject;
import java.util.List;

/**
 * ShardedJob pipeline.
 *
 * NOTE: can't use injection bc of type erasure; error is :
 *   "Cannot inject members into raw type com.google.appengine.tools.mapreduce.impl.pipeline.ShardedJob"
 *
 * ideas:
 *   - eliminate type of this?
 *   - fill dependencies at runtime via context of Job? eg, give it a handle to the container
 *          - PipelineContainer interface that any container extends
 *
 *
 * @param <T> type of task
 */
@RequiredArgsConstructor
public class ShardedJob<T extends IncrementalTask> extends Job0<Void> {

  private static final long serialVersionUID = 1L;

  // alternatively, put namespace on SharedJobSettings; project+databaseId+namespace there; or something else
  @NonNull private final DatastoreOptions datastoreOptions;
  @NonNull private final String jobId;
  @NonNull private final List<? extends T> workers;
  @NonNull private final ShardedJobController<T> controller;
  @NonNull private final ShardedJobSettings settings;


  @Override
  public Value<Void> run() {
    Datastore datastore = datastoreOptions.getService();
    getPipelineOrchestrator().startJob(datastore, jobId, workers, controller, settings);
    setStatusConsoleUrl(settings.getMapReduceStatusUrl());
    return null;
  }
}
