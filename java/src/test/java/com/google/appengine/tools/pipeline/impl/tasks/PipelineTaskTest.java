package com.google.appengine.tools.pipeline.impl.tasks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.cloud.datastore.Key;

class PipelineTaskTest {

    @Test
    void testToAndFromPropertiesWithDatastoreSettings() {
        QueueSettings queueSettings = new QueueSettings();
        queueSettings.setDatabaseId("my-database");
        queueSettings.setNamespace("my-namespace");

        Key dummyKey = Key.newBuilder("my-project", "MyKind", "my-key").build();
        RunJobTask task = new RunJobTask(dummyKey, queueSettings);

        Properties properties = task.toProperties();

        assertEquals("my-database", properties.getProperty("dsDatabaseId"));
        assertEquals("my-namespace", properties.getProperty("dsNamespace"));
        assertEquals(PipelineTask.Type.RUN_JOB.name(), properties.getProperty(PipelineTask.TASK_TYPE_PARAMETER));

        PipelineTask reconstructed = PipelineTask.fromProperties(task.getTaskName(), properties);
        assertNotNull(reconstructed);
        assertEquals("my-database", reconstructed.getQueueSettings().getDatabaseId());
        assertEquals("my-namespace", reconstructed.getQueueSettings().getNamespace());
    }

    @Test
    void testToAndFromPropertiesWithoutDatastoreSettings() {
        QueueSettings queueSettings = new QueueSettings();

        Key dummyKey = Key.newBuilder("my-project", "MyKind", "my-key").build();
        RunJobTask task = new RunJobTask(dummyKey, queueSettings);

        Properties properties = task.toProperties();

        assertNull(properties.getProperty("dsDatabaseId"));
        assertNull(properties.getProperty("dsNamespace"));

        PipelineTask reconstructed = PipelineTask.fromProperties(task.getTaskName(), properties);
        assertNotNull(reconstructed);
        assertNull(reconstructed.getQueueSettings().getDatabaseId());
        assertNull(reconstructed.getQueueSettings().getNamespace());
    }
}
