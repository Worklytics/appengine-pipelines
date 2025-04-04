// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.JobRunId;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.PipelineTask;
import com.google.appengine.tools.pipeline.util.Pair;
import com.google.cloud.datastore.Key;

import java.io.Serializable;
import java.util.Set;

/**
 * An interface that gives access to data store and task queue operations that
 * must be performed during the execution of a Pipeline.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface PipelineBackEnd {

  Options getOptions();

  /**
   * Saves entities to the data store and enqueues tasks to the task queue based
   * on the specification given in {@code UpdateSpec}. See the remarks at the
   * top of {@link UpdateSpec} for details.
   */
  void save(UpdateSpec updateSpec);

  /**
   * Saves an {@code UpdateSpec} to the data store, but transactionally checks
   * that a certain condition is true before committing the final transaction.
   * <p>
   * See the remarks at the top of {@link UpdateSpec} for more information about
   * {@code UpdateSpecs}. As part of the
   * {@link UpdateSpec#getFinalTransaction() final transaction} the
   * {@link JobRecord} with the given {@code jobKey} will be retrieved from the
   * data store and its {@link JobRecord#getState() state} will be checked to
   * see if it is one of the {@code expectedStates}. If not then the final
   * transaction will be aborted, and this method will return {@code false}.
   * @return {@code true} iff the transaction was applied successfully.
   */
  boolean saveWithJobStateCheck(UpdateSpec updateSpec,
                                Key jobKey, JobRecord.State... expectedStates);

  /**
   * Get the JobRecord with the given Key from the data store, and optionally
   * also get some of the Barriers and Slots associated with it.
   *
   * @param key The key of the JobRecord to be fetched
   * @param inflationType Specifies the manner in which the returned JobRecord
   *        should be inflated.
   * @return A {@code JobRecord}, possibly with a partially-inflated associated
   *         graph of objects.
   * @throws NoSuchObjectException If Either the JobRecord or any of the
   *         associated Slots or Barriers are not found in the data store.
   */
  JobRecord queryJob(Key key, JobRecord.InflationType inflationType) throws NoSuchObjectException;

  /**
   * Get the Slot with the given Key from the data store, and optionally also
   * get the Barriers that are waiting on the Slot, and the other Slots that
   * those Barriers are waiting on.
   *
   * @param key The Key of the slot to fetch.
   * @param inflate If this is {@code true} then the Barriers that are waiting
   *        on the Slot and the other Slots that those Barriers are waiting on
   *        will also be fetched from the data store and used to partially
   *        populate the graph of objects attached to the returned Slot. In
   *        particular: {@link Slot#getWaitingOnMeInflated()} will not return
   *        {@code null} and also that for each of the
   *        {@link com.google.appengine.tools.pipeline.impl.model.Barrier Barriers}
   *        returned from that method
   *        {@link com.google.appengine.tools.pipeline.impl.model.Barrier#getWaitingOnInflated()}
   *        will not return {@code null}.
   * @return A {@code Slot}, possibly with a partially-inflated associated graph
   *         of objects.
   * @throws NoSuchObjectException
   */
  Slot querySlot(Key key, boolean inflate) throws NoSuchObjectException;

  /**
   * Get the Failure with the given Key from the data store.
   *
   * @param key The Key of the failure to fetch.
   * @return A {@code FailureRecord}
   * @throws NoSuchObjectException
   */
  ExceptionRecord queryFailure(Key key) throws NoSuchObjectException;

  /**
   * Queries the data store for all Pipeline objects associated with the given
   * root Job Key
   */
  PipelineObjects queryFullPipeline(Key rootJobKey);

  /**
   * Delete all datastore entities corresponding to the given pipeline.
   *
   * @param pipelineRunId The root job key identifying the pipeline
   * @param force         If this parameter is not {@code true} then this method will
   *                      throw an {@link IllegalStateException} if the specified pipeline is
   *                      not in the
   *                      {@link JobRecord.State#FINALIZED}
   *                      or
   *                      {@link JobRecord.State#STOPPED}
   *                      state.
   * @throws IllegalStateException If {@code force = false} and the specified
   *                               pipeline is not in the
   *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED}
   *                               or
   *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED}
   *                               state.
   */
  void deletePipeline(JobRunId pipelineRunId, boolean force)
      throws IllegalStateException;

  /**
   * Immediately enqueues the given task in the app engine task queue. Note that
   * there is another way to enqueue a task, namely to register the task with
   * {@link UpdateSpec.TransactionWithTasks#registerTask} that is associated
   * with the {@link UpdateSpec#getFinalTransaction() final transaction} of an
   * {@link UpdateSpec}. This method is simpler if one only wants to enqueue a
   * single task in isolation.
   * <p>
   * NOTE: will never return TaskAlreadyExistsException (which can only occur with named tasks)
   *
   * @return name of task in the queue
   */
  PipelineTaskQueue.TaskReference enqueue(PipelineTask pipelineTask);

  /**
   * Queries the data store for all root Pipeline.
   *
   * @param classFilter An optional filter by class display name.
   * @param cursor An optional cursor (used for paging).
   * @param limit Results limit (zero or negative will be treated as no limit).
   * @return a Pair of job records and a next cursor (or null, if no more results).
   */
  Pair<? extends Iterable<JobRecord>, String> queryRootPipelines(
      String classFilter, String cursor, int limit);

  /**
   * Returns the set of all root pipelines display name.
   */
  Set<String> getRootPipelinesDisplayName();

  /**
   * @return SerializationStrategy to be used for this backend
   */
  SerializationStrategy getSerializationStrategy();

  /**
   * serializable configuration of a PipelineBackend, so can be re-constituted in another context
   */
  interface Options extends Serializable {

    default <T extends Options> T as(Class<T> tClass) {
      return (tClass.cast(this));
    }

  }

  String getDefaultService();

  String getDefaultVersion(String service);
}

