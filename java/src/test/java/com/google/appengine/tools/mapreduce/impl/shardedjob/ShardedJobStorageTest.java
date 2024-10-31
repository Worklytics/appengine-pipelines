package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.appengine.tools.mapreduce.EndToEndTestCase;

import com.google.cloud.datastore.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;

/**
 * Tests the format in which ShardedJobs are written to the datastore.
 *
 */
public class ShardedJobStorageTest extends EndToEndTestCase {


  ShardedJobRunId jobId;

  @BeforeEach
  public void fillJobId() {
    jobId = shardedJobId("partition_id+%7B%0A++project_id%3A+%22test-project%22%0A%7D%0Apath+%7B%0A++kind%3A+%22pipeline-job%22%0A++name%3A+%22c6fa877b-81a6-4e17-a8f7-62268036db97%22%0A%7D%0A");
  }

  @Test
  public void testRoundTripJob() {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Transaction tx = getDatastore().newTransaction();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, job);
    tx.put(entity);
    tx.commit();

    Transaction readTx = getDatastore().newTransaction();

    Entity readEntity = readTx.get(entity.getKey());
    assertEquals(entity, readEntity);
    ShardedJobStateImpl<TestTask> fromEntity =
        ShardedJobStateImpl.ShardedJobSerializer.fromEntity(readTx, readEntity);
    assertEquals(job.getShardedJobId(), fromEntity.getShardedJobId());
    assertEquals(job.getActiveTaskCount(), fromEntity.getActiveTaskCount());
    assertEquals(job.getMostRecentUpdateTime().truncatedTo(ChronoUnit.MILLIS), fromEntity.getMostRecentUpdateTime().truncatedTo(ChronoUnit.MILLIS));
    assertEquals(job.getStartTime().truncatedTo(ChronoUnit.MILLIS), fromEntity.getStartTime().truncatedTo(ChronoUnit.MILLIS));
    assertEquals(job.getTotalTaskCount(), fromEntity.getTotalTaskCount());
    assertEquals(job.getSettings().toString(), fromEntity.getSettings().toString());
    assertEquals(job.getStatus(), fromEntity.getStatus());
    assertEquals(job.getController(), fromEntity.getController());
  }

  @Test
  public void testExpectedFields() {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Transaction tx = getDatastore().newTransaction();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, job);
    assertEquals(10, entity.getLong("taskCount"));
    assertTrue(entity.contains("activeShards"));
    assertTrue(entity.contains("status"));
    assertTrue(entity.contains("startTime"));
    assertTrue(entity.contains("settings"));
    assertTrue(entity.contains("mostRecentUpdateTime"));
  }

  @Test
  public void testFetchJobById() {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Transaction tx = getDatastore().newTransaction();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, job);
    tx.put(entity);
    tx.commit();

    Entity readEntity = getDatastore().get(ShardedJobStateImpl.ShardedJobSerializer.makeKey(getDatastore(), jobId));
    assertEquals(entity, readEntity);
  }

  private ShardedJobStateImpl<TestTask> createGenericJobState() {
    return ShardedJobStateImpl.create(jobId, new TestController(getDatastore().getOptions(), 11, getPipelineService(), false),
        new ShardedJobSettings.Builder().build(), 10, Instant.now());
  }

  @Test
  public void testQueryByKind() {
    Query<Entity> query = Query.newEntityQueryBuilder()
      .setKind(ShardedJobStateImpl.ShardedJobSerializer.ENTITY_KIND)
      .build();
    Iterator<Entity> iterable = getDatastore().run(query);
    assertFalse(iterable.hasNext());

    ShardedJobStateImpl<TestTask> job = createGenericJobState();

    Transaction tx = getDatastore().newTransaction();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, job);
    tx.put(entity);
    tx.commit();

    QueryResults<Entity> expectOne = getDatastore().run(query);
    Entity singleEntity = expectOne.next();
    assertEquals(entity, singleEntity);
  }
}
