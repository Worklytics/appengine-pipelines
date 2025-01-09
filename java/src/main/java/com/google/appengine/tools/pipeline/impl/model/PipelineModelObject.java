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

package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.tools.pipeline.impl.util.EntityUtils;
import com.google.cloud.datastore.*;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;
import lombok.NonNull;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The parent class of all Pipeline model objects.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public abstract class PipelineModelObject {

  public static final String ROOT_JOB_KEY_PROPERTY = "rootJobKey";
  private static final String GENERATOR_JOB_PROPERTY = "generatorJobKey";
  private static final String GRAPH_GUID_PROPERTY = "graphGUID";

  /**
   * Datastore key of this object
   */
  private final Key key;

  /**
   * Datastore key of the root job identifying the Pipeline to which this object
   * belongs.
   */
  private final Key rootJobKey;

  /**
   * Datastore key of the generator job of this object. Your generator job is
   * the job whose run() method created you and the rest of your local job
   * graph. The generator of the objects in the root job graph is null.
   */
  private final Key generatorJobKey;

  /**
   * A GUID generated during the execution of a run() method of a generator job.
   * Each of the objects in a local job graph are marked with this GUID and the
   * generator job records the graphGUID of each of it's child job graph. This
   * enables us to distinguish between a valid child job graph and one that is
   * orphaned. A child job graph is valid if its graphGUID is equal to the
   * childGraphGUID of its generator job.
   */
  private final String graphGUID;

  /**
   * Construct a new PipelineModelObject from the provided data.
   *
   * @param rootJobKey The key of the root job for this pipeline. This must be
   *        non-null, except in the case that we are currently constructing the
   *        root job. In that case {@code thisKey} and {@code egParentKey} must
   *        both be null and this must be a {@link JobRecord}.
   * @param egParentKey The entity group parent key. This must be null unless
   *        {@code thisKey} is null. If {@code thisKey} is null then
   *        {@code parentKey} will be used to construct {@code thisKey}.
   *        {@code parentKey} and {@code thisKey} are both allowed to be null,
   *        in which case {@code thisKey} will be constructed without a parent.
   * @param thisKey The key for the object being constructed. If this is null
   *        then a new key will be constructed.
   * @param generatorJobKey The key of the job whose run() method created this
   *        object. This must be non-null unless this object is part of the root
   *        job graph---i.e. the root job, or one of its barriers or slots.
   * @param graphGUID The unique GUID of the local graph of this object. This is
   *        used to determine whether or not this object is orphaned. The object
   *        is defined to be non-orphaned if its graphGUID is equal to the
   *        childGraphGUID of its parent job. This must be non-null unless this
   *        object is part of the root job graph---i.e. the root job, or one of
   *        its barriers or slots.
   */
  protected PipelineModelObject(
      Key rootJobKey, Key egParentKey, Key thisKey, Key generatorJobKey, String graphGUID) {
    if (null == rootJobKey) {
      throw new IllegalArgumentException("rootJobKey is null");
    }
    if (generatorJobKey == null && graphGUID != null ||
        generatorJobKey != null && graphGUID == null) {
      throw new IllegalArgumentException(
          "Either neither or both of generatorParentJobKey and graphGUID must be set.");
    }

    this.rootJobKey = rootJobKey;
    this.generatorJobKey = generatorJobKey;
    this.graphGUID = graphGUID;
    if (null == thisKey) {
      if (egParentKey == null) {
        key = generateKey(rootJobKey.getProjectId(), rootJobKey.getNamespace(), getDatastoreKind());
      } else {
        key = generateKey(egParentKey, getDatastoreKind());
      }
    } else {
      if (egParentKey != null) {
        throw new IllegalArgumentException("You may not specify both thisKey and parentKey");
      }
      key = thisKey;
    }
  }

  /**
   * Construct a new PipelineModelObject with the given rootJobKey,
   * generatorJobKey, and graphGUID, a newly generated key, and no entity group
   * parent.
   *
   * @param rootJobKey The key of the root job for this pipeline. This must be
   *        non-null, except in the case that we are currently constructing the
   *        root job. In that case this must be a {@link JobRecord}.
   * @param generatorJobKey The key of the job whose run() method created this
   *        object. This must be non-null unless this object is part of the root
   *        job graph---i.e. the root job, or one of its barriers or slots.
   * @param graphGUID The unique GUID of the local graph of this object. This is
   *        used to determine whether or not this object is orphaned. The object
   *        is defined to be non-orphaned if its graphGUID is equal to the
   *        childGraphGUID of its parent job. This must be non-null unless this
   *        object is part of the root job graph---i.e. the root job, or one of
   *        its barriers or slots.
   */
  protected PipelineModelObject(Key rootJobKey, Key generatorJobKey, String graphGUID) {
    this(rootJobKey, null, null, generatorJobKey, graphGUID);
  }

  /**
   * Construct a new PipelineModelObject from the previously saved Entity.
   *
   * @param entity An Entity obtained previously from a call to
   *    {@link #toEntity()}.
   */
  protected PipelineModelObject(Entity entity) {
    this(extractRootJobKey(entity), null, extractKey(entity), extractGeneratorJobKey(entity),
        extractGraphGUID(entity));
    String expectedEntityType = getDatastoreKind();
    if (!expectedEntityType.equals(extractType(entity))) {
      throw new IllegalArgumentException("The entity is not of kind " + expectedEntityType);
    }
  }

  protected static Key generateKey(Key parentKey, String kind) {
    String name = GUIDGenerator.nextGUID();

    KeyFactory keyFactory = new KeyFactory(parentKey.getProjectId(), parentKey.getNamespace());
    keyFactory.addAncestors(parentKey.getAncestors());
    keyFactory.addAncestor(PathElement.of(parentKey.getKind(), parentKey.getName()));
    keyFactory.setKind(kind);
    return keyFactory.newKey(name);
  }


  public static Key generateKey(@NonNull String projectId, String namespace, @NonNull String dataStoreKind) {

    //ISO date + time (to second) as a suffix, to aid human readability/traceability; any place we log job id, we know when it was triggered

    // q: why not swap it to a prefix?
    // pro:
    //   - even easier to read
    //  - free index by time
    // con:
    //  - index will be hot

    // TODO: swap this once have per-tenant database/namespace, which should limit the index overheat issue
    String name =
      GUIDGenerator.nextGUID().replace("-", "") //avoid collision
      + "_" +
    Instant.now().truncatedTo(ChronoUnit.SECONDS).toString()
        .replace(":", "")
        .replace("T", "_")
        .replace("Z", "")
        .replace("-", "");

    KeyFactory keyFactory = new KeyFactory(projectId);
    if (namespace != null ) { //null implies default
      keyFactory.setNamespace(namespace);
    }
    keyFactory.setKind(dataStoreKind);
    return keyFactory.newKey(name);
  }

  private static Key extractRootJobKey(Entity entity) {
    return entity.getKey(ROOT_JOB_KEY_PROPERTY);
  }

  private static Key extractGeneratorJobKey(Entity entity) {
    return EntityUtils.getKey(entity, GENERATOR_JOB_PROPERTY);
  }

  private static String extractGraphGUID(Entity entity) {
    return EntityUtils.getString(entity, GRAPH_GUID_PROPERTY);
  }

  private static String extractType(Entity entity) {
    return entity.getKey().getKind();
  }

  private static Key extractKey(Entity entity) {
    return entity.getKey();
  }

  public abstract Entity toEntity();

  protected Entity.Builder toProtoBuilder() {
    Entity.Builder builder = Entity.newBuilder(key);
    builder.set(ROOT_JOB_KEY_PROPERTY, rootJobKey);
    if (generatorJobKey != null) {
      builder.set(GENERATOR_JOB_PROPERTY, generatorJobKey);
    }
    if (graphGUID != null) {
      builder.set(GRAPH_GUID_PROPERTY, graphGUID);
    }
    return builder;
  }

  public Key getKey() {
    return key;
  }

  public Key getRootJobKey() {
    return rootJobKey;
  }

  public Key getGeneratorJobKey() {
    return generatorJobKey;
  }

  public String getGraphGuid() {
    return graphGUID;
  }

  protected abstract String getDatastoreKind();

  protected static <E> List<E> buildInflated(Collection<Key> listOfIds, Map<Key, E> pool) {
    ArrayList<E> list = new ArrayList<>(listOfIds.size());
    for (Key id : listOfIds) {
      E x = pool.get(id);
      if (null == x) {
        throw new RuntimeException("No object found in pool with id=" + id);
      }
      list.add(x);
    }
    return list;
  }

  protected static <E> List<E> getListProperty(String propertyName, Entity entity) {
    if (entity.contains(propertyName)) {
      return (List<E>) entity.getList(propertyName).stream()
        .map(Value::get)
        .collect(Collectors.toCollection(ArrayList::new));
    } else {
      return new LinkedList<>();
    }
  }

  protected static String getKeyName(Key key) {
    return key == null ? "null" : key.getName();
  }
}
