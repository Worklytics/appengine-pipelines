package com.google.appengine.tools.pipeline.impl.model;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.TimestampValue;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * a java model corresponding to a Datastore entity that supports a TTL (time-to-live) expiration policy
 *
 * see: https://cloud.google.com/datastore/docs/ttl
 *
 * implementers, apart from implementing this interface, should also ensure they use the provided helper methods to:
 *   -  fill the datastore property on entities before being sent to datastore
 *   - recover the value of the datastore property on entities upon retrieval from the datastore
 *   - if desired, initialize the expireAt property in the constructor of the implementing class
 *
 */
public interface ExpiringDatastoreEntity {

  /**
   * time at which this entity should expire, and be cleaned up from the datastore, if any.
   *
   * null implies no expiration (entity will not be automatically cleaned up)
   *
   * NOTE: will NOT be enforced unless you also configure retention TTL policy on your datastore instance.
   *
   */
  @Nullable
  Instant getExpireAt();

  /**
   * time at which this entity should expire, and be cleaned up from the datastore, if any.
   *
   * null implies no expiration (entity will not be automatically cleaned up)
   *
   * NOTE: will NOT be enforced unless you also configure retention TTL policy on your datastore instance.
   *
   * see: https://cloud.google.com/datastore/docs/ttl
   */
  void setExpireAt(@Nullable Instant expireAt);

  /**
   * All entities created by this library should have this property filled with Timestamp == creation time + TTL value (below),
   *
   * This is time at which this entity should expire, and be cleaned up from the datastore, if any.
   *
   * null implies no expiration (entity will not be automatically cleaned up)
   *
   * will NOT be enforced unless you also configure retention TTL policy on your datastore instance.
   *
   * see: https://cloud.google.com/datastore/docs/ttl
   */
  String EXPIRE_AT_PROPERTY = "expireAt";

  /**
   * added to `expireAt` by default to give an expiration time
   */
  Duration DEFAULT_TTL_PERIOD = Duration.ofDays(90);

  /**
   * helper to get the expireAt property from an entity, if it exists (nullable)
   * @param in datastore entity to check, presumably of a kind whose corresponding Java model implements this interface
   * @return the expireAt property as an Instant, if it exists, or null
   */
  static Instant getExpireAt(Entity in) {
    if (in.contains(EXPIRE_AT_PROPERTY)) {
      return Instant.ofEpochSecond(in.getTimestamp(EXPIRE_AT_PROPERTY).getSeconds());
    } else {
      return null;
    }
  }

  /**
   * @return default value for expireAt; can override if you want something different for a given kind
   */
  default Instant defaultExpireAt() {
    return Instant.now().plus(DEFAULT_TTL_PERIOD);
  }

  default void fillExpireAt(Entity.Builder entityBuilder) {
    if (getExpireAt() != null) {
      //NOTE: best practices per Datastore docs is to NOT index this, bc creates hotspots
      // (you're presumably always writing to tail of the index, and always removing from head)
      // see https://cloud.google.com/datastore/docs/ttl#ttl_properties_and_indexes
      entityBuilder.set(EXPIRE_AT_PROPERTY, TimestampValue.newBuilder(Timestamp.of(Date.from(getExpireAt())))
        .setExcludeFromIndexes(true).build());
    }
  }

}
