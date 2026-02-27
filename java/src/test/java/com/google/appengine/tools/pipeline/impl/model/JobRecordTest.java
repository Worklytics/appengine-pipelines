package com.google.appengine.tools.pipeline.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    }

    @Test
    void testSubJobRecordInheritsGeneratorSettings() {
        Job<?> jobInstance = mock(Job.class);
        SerializationStrategy serializationStrategy = mock(SerializationStrategy.class);

        Key rootJobKey = Key.newBuilder("my-project", "JobRecord", "root-job").build();
        Key generatorJobKey = Key.newBuilder("my-project", "JobRecord", "gen-job")
                .setDatabaseId("gen-db")
                .setNamespace("gen-ns")
                .build();

        JobRecord mockGenerator = mock(JobRecord.class);
        when(mockGenerator.getRootJobKey()).thenReturn(rootJobKey);
        when(mockGenerator.getKey()).thenReturn(generatorJobKey);

        JobRecord subJob = new JobRecord(mockGenerator, "graph-id",
                jobInstance, false, new JobSetting[0], serializationStrategy);

        assertEquals("gen-db", subJob.getDatabaseId());
        assertEquals("gen-ns", subJob.getNamespace());
    }

    @Test
    void testSubJobRecordOverridesGeneratorSettings() {
        Job<?> jobInstance = mock(Job.class);
        SerializationStrategy serializationStrategy = mock(SerializationStrategy.class);

        Key rootJobKey = Key.newBuilder("my-project", "JobRecord", "root-job").build();
        Key generatorJobKey = Key.newBuilder("my-project", "JobRecord", "gen-job")
                .setDatabaseId("gen-db")
                .setNamespace("gen-ns")
                .build();

        JobRecord mockGenerator = mock(JobRecord.class);
        when(mockGenerator.getRootJobKey()).thenReturn(rootJobKey);
        when(mockGenerator.getKey()).thenReturn(generatorJobKey);

        JobSetting.DatastoreDatabase dbSetting = new JobSetting.DatastoreDatabase("new-db");
        JobSetting.DatastoreNamespace nsSetting = new JobSetting.DatastoreNamespace("new-ns");
        JobSetting[] settings = new JobSetting[] { dbSetting, nsSetting };

        JobRecord subJob = new JobRecord(mockGenerator, "graph-id",
                jobInstance, false, settings, serializationStrategy);

        assertEquals("new-db", subJob.getDatabaseId());
        assertEquals("new-ns", subJob.getNamespace());
    }
}
