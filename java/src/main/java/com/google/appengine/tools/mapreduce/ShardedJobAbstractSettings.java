package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.pipeline.JobSetting;
import com.google.cloud.datastore.DatastoreOptions;

import java.util.Optional;

/**
 * interface to represent settings of a sharded job
 *
 * NOTE: ShardedJobSettings are should be renamed to ShardedJobRunSettings or something like that, as it's particular to a given run of the job
 *
 * this is common to *multiple* runs of the job (eg, if it's run multiple times, this is generally always the same)
 */
public interface ShardedJobAbstractSettings {


  String getDatastoreHost();

  String getProjectId();

  String getDatabaseId();

  String getNamespace();

  String getBaseUrl();

  String getModule();

  String getWorkerQueueName();

  int getMaxShardRetries();

  int getMaxSliceRetries();

  int getMillisPerSlice();

  double getSliceTimeoutRatio();


  default JobSetting[] toJobSettings(JobSetting... extra) {
    JobSetting[] settings = new JobSetting[3 + extra.length];
    settings[0] = new JobSetting.OnService(getModule());
    settings[1] = new JobSetting.OnQueue(getWorkerQueueName());
    settings[2] = new JobSetting.DatastoreNamespace(getNamespace());
    System.arraycopy(extra, 0, settings, 3, extra.length);
    return settings;
  }


  default DatastoreOptions getDatastoreOptions() {
    DatastoreOptions.Builder optionsBuilder = DatastoreOptions.getDefaultInstance().toBuilder();
    Optional.ofNullable(getDatastoreHost()).ifPresent(optionsBuilder::setHost);
    Optional.ofNullable(getProjectId()).ifPresent(optionsBuilder::setProjectId);
    Optional.ofNullable(getDatabaseId()).ifPresent(optionsBuilder::setDatabaseId);
    Optional.ofNullable(getNamespace()).ifPresent(optionsBuilder::setNamespace);
    return optionsBuilder.build();
  }
}
