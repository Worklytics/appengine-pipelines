package com.google.appengine.tools.pipeline.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.impl.backend.SerializationStrategy;
import com.google.cloud.datastore.Key;

class JobRecordTest {

    @Test
    void testRootJobRecordSettings() {
        Job<?> jobInstance = mock(Job.class);
        SerializationStrategy serializationStrategy = mock(SerializationStrategy.class);

        JobSetting.DatastoreDatabase dbSetting = new JobSetting.DatastoreDatabase("my-db");
        JobSetting.DatastoreNamespace nsSetting = new JobSetting.DatastoreNamespace("my-ns");

        JobSetting[] settings = new JobSetting[] { dbSetting, nsSetting };

        JobRecord rootJob = JobRecord.createRootJobRecord("my-project", jobInstance, serializationStrategy, settings);

        assertEquals("my-db", rootJob.getDatabaseId());
        assertEquals("my-ns", rootJob.getNamespace());
        assertEquals("my-db", rootJob.getKey().getDatabaseId());
        assertEquals("my-ns", rootJob.getKey().getNamespace());
        assertEquals("my-db", rootJob.getQueueSettings().getDatabaseId());
        assertEquals("my-ns", rootJob.getQueueSettings().getNamespace());
    }

    @Test
    void testSubJobRecordInheritsGeneratorSettings() {
        Job<?> jobInstance = mock(Job.class);
        SerializationStrategy serializationStrategy = mock(SerializationStrategy.class);

        Key rootJobKey = Key.newBuilder("my-project", "JobRecord", "root-job")
                .setDatabaseId("root-db")
                .setNamespace("root-ns")
                .build();
        Key generatorJobKey = Key.newBuilder("my-project", "JobRecord", "gen-job")
                .setDatabaseId("root-db")
                .setNamespace("root-ns")
                .build();

        JobRecord mockGenerator = mock(JobRecord.class);
        when(mockGenerator.getRootJobKey()).thenReturn(rootJobKey);
        when(mockGenerator.getKey()).thenReturn(generatorJobKey);
        when(mockGenerator.getQueueSettings()).thenReturn(new com.google.appengine.tools.pipeline.impl.QueueSettings());

        JobRecord subJob = new JobRecord(mockGenerator, "graph-id",
                jobInstance, false, new JobSetting[0], serializationStrategy);

        assertEquals("root-db", subJob.getDatabaseId());
        assertEquals("root-ns", subJob.getNamespace());
        assertEquals("root-db", subJob.getKey().getDatabaseId());
        assertEquals("root-ns", subJob.getKey().getNamespace());
        assertEquals("root-db", subJob.getQueueSettings().getDatabaseId());
        assertEquals("root-ns", subJob.getQueueSettings().getNamespace());
    }

    @Test
    void testSubJobRecordOverridesGeneratorSettings() {
        Job<?> jobInstance = mock(Job.class);
        SerializationStrategy serializationStrategy = mock(SerializationStrategy.class);

        Key rootJobKey = Key.newBuilder("my-project", "JobRecord", "root-job")
                .setDatabaseId("root-db")
                .setNamespace("root-ns")
                .build();
        Key generatorJobKey = Key.newBuilder("my-project", "JobRecord", "gen-job")
                .setDatabaseId("root-db")
                .setNamespace("root-ns")
                .build();

        JobRecord mockGenerator = mock(JobRecord.class);
        when(mockGenerator.getRootJobKey()).thenReturn(rootJobKey);
        when(mockGenerator.getKey()).thenReturn(generatorJobKey);
        when(mockGenerator.getQueueSettings()).thenReturn(new com.google.appengine.tools.pipeline.impl.QueueSettings());

        JobSetting.DatastoreDatabase dbSetting = new JobSetting.DatastoreDatabase("new-db");
        JobSetting.DatastoreNamespace nsSetting = new JobSetting.DatastoreNamespace("new-ns");
        JobSetting[] settings = new JobSetting[] { dbSetting, nsSetting };

        assertThrows(IllegalArgumentException.class, () -> new JobRecord(mockGenerator, "graph-id",
                jobInstance, false, settings, serializationStrategy));
    }
}
