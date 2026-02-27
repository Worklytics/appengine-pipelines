package com.google.appengine.tools.pipeline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class JobSettingTest {

    @Test
    void testDatastoreDatabaseValidation() {
        assertDoesNotThrow(() -> new JobSetting.DatastoreDatabase(null));
        assertDoesNotThrow(() -> new JobSetting.DatastoreDatabase(""));
        assertDoesNotThrow(() -> new JobSetting.DatastoreDatabase("(default)"));
        assertDoesNotThrow(() -> new JobSetting.DatastoreDatabase("my-database-123"));

        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class,
                () -> new JobSetting.DatastoreDatabase("123-starts-with-number"));
        assertEquals("Invalid Datastore database ID: 123-starts-with-number", e1.getMessage());

        assertThrows(IllegalArgumentException.class, () -> new JobSetting.DatastoreDatabase("UPPERCASE"));
        assertThrows(IllegalArgumentException.class, () -> new JobSetting.DatastoreDatabase("contains space"));
        assertThrows(IllegalArgumentException.class, () -> new JobSetting.DatastoreDatabase("a".repeat(64))); // max
                                                                                                              // length
                                                                                                              // 63
    }

    @Test
    void testDatastoreNamespaceValidation() {
        assertDoesNotThrow(() -> new JobSetting.DatastoreNamespace(null));
        assertDoesNotThrow(() -> new JobSetting.DatastoreNamespace(""));
        assertDoesNotThrow(() -> new JobSetting.DatastoreNamespace("my-namespace_1.0"));

        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class,
                () -> new JobSetting.DatastoreNamespace("invalid space"));
        assertEquals("Invalid Datastore namespace: invalid space", e1.getMessage());

        assertThrows(IllegalArgumentException.class, () -> new JobSetting.DatastoreNamespace("a".repeat(101))); // max
                                                                                                                // length
                                                                                                                // 100
    }
}
