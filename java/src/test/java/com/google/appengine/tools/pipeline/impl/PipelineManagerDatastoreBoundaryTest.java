package com.google.appengine.tools.pipeline.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.inject.Provider;

import org.junit.jupiter.api.Test;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.DatastoreOptions;

class PipelineManagerDatastoreBoundaryTest {

  @Test
  void absentSettingInheritsTheBackendPartition() {
    PipelineManager manager = managerBoundTo("tenant-db", "tenant-ns");

    JobSetting[] resolved = manager.datastoreBoundaryFromBackend(new JobSetting[0]);

    assertEquals("tenant-db",
        JobSetting.getSettingValue(JobSetting.DatastoreDatabase.class, resolved).orElseThrow());
    assertEquals("tenant-ns",
        JobSetting.getSettingValue(JobSetting.DatastoreNamespace.class, resolved).orElseThrow());
  }

  @Test
  void explicitNullStaysOnTheDefaultPartition() {
    PipelineManager manager = managerBoundTo("tenant-db", "tenant-ns");

    JobSetting[] resolved = manager.datastoreBoundaryFromBackend(new JobSetting[] {
        new JobSetting.DatastoreDatabase(null),
        new JobSetting.DatastoreNamespace(null)
    });

    assertEquals(2, resolved.length);
    assertTrue(JobSetting.findSetting(JobSetting.DatastoreDatabase.class, resolved).isPresent());
    assertNull(JobSetting.getSettingValue(JobSetting.DatastoreDatabase.class, resolved).orElse(null));
    assertNull(JobSetting.getSettingValue(JobSetting.DatastoreNamespace.class, resolved).orElse(null));
  }

  private static PipelineManager managerBoundTo(String databaseId, String namespace) {
    DatastoreOptions datastoreOptions = DatastoreOptions.newBuilder()
        .setProjectId("test-project")
        .setDatabaseId(databaseId)
        .setNamespace(namespace)
        .setCredentials(NoCredentials.getInstance())
        .build();
    AppEngineBackEnd.Options options = AppEngineBackEnd.Options.builder()
        .projectId("test-project")
        .datastoreOptions(datastoreOptions)
        .build();
    PipelineBackEnd backend = mock(PipelineBackEnd.class);
    when(backend.getOptions()).thenReturn(options);
    return new PipelineManager(mock(Provider.class), mock(ShardedJobRunner.class), backend);
  }
}
