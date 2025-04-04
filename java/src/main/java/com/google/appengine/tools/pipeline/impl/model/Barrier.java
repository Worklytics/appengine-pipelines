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

import com.google.cloud.datastore.*;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import lombok.Getter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A {@code Barrier} represents a list of slots that need to be filled before
 * something is allowed to happen.
 * <p>
 * There are two types of barriers, run barriers and finalize barriers. A run
 * barrier is used to trigger the running of a job. Its list of slots represent
 * arguments to the job. A finalize barrier is used to trigger the finalization
 * of a job. It has only one slot which is used as the output value of the job.
 * The essential properties are:
 * <ul>
 * <li>type: Either run or finalize
 * <li>jobKey: The datastore key of the associated job
 * <li>waitingOn: A list of the datastore keys of the slots for which this
 * barrier is waiting
 * <li>released: A boolean representing whether or not this barrier is released.
 * Released means that all of the slots are filled and so the action associated
 * with this barrier should be triggered.
 * </ul>
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class Barrier extends PipelineModelObject implements ExpiringDatastoreEntity {

  public static final String DATA_STORE_KIND = "pipeline-barrier";

  /**
   * The type of Barrier
   */
  public enum Type {
    RUN, // these are root-level entities in datastore
    FINALIZE  //NOTE: these have parents (belong to entity group of job they finalize)
  }

  private static final String TYPE_PROPERTY = "barrierType";
  private static final String JOB_KEY_PROPERTY = "jobKey";
  private static final String RELEASED_PROPERTY = "released";
  private static final String WAITING_ON_KEYS_PROPERTY = "waitingOnKeys";
  private static final String WAITING_ON_GROUP_SIZES_PROPERTY = "waitingOnGroupSizes";

  // persistent
  @Getter
  private final Type type;
  @Getter
  private final Key jobKey;
  @Getter
  private boolean released;

  /**
   * may be null if this Barrier has not been inflated
   */
  @Getter
  private final List<Key> waitingOnKeys;
  private final List<Long> waitingOnGroupSizes;

  /**
   * -- GETTER --
   *  May return null if this Barrier has not been inflated
   */
  // transient
  @Getter
  private List<SlotDescriptor> waitingOnInflated;

  /**
   * Returns the entity group parent of a Barrier of the specified type.
   * <p>
   * According to our <a href="http://goto/java-pipeline-model">transactional
   * model</a>: If B is the finalize barrier of a Job J, then the entity group
   * parent of B is J. Run barriers do not have an entity group parent.
   */
  private static Key getEgParentKey(Type type, Key jobKey) {
    switch (type) {
      case RUN:
        return null;
      case FINALIZE:
        if (null == jobKey) {
          throw new IllegalArgumentException("jobKey is null");
        }
        break;
    }
    return jobKey;
  }

  private Barrier(Type type, Key rootJobKey, Key jobKey, Key generatorJobKey, String graphGUID) {
    super(rootJobKey, getEgParentKey(type, jobKey), null, generatorJobKey, graphGUID);
    this.jobKey = jobKey;
    this.type = type;
    waitingOnGroupSizes = new LinkedList<>();
    waitingOnInflated = new LinkedList<>();
    waitingOnKeys = new LinkedList<>();
  }

  public static Barrier dummyInstanceForTesting() {
    Key dummyKey = Key.newBuilder("dummy", "dummy", "dummy").build();
    return new Barrier(Type.RUN, dummyKey, dummyKey, dummyKey, "abc");
  }

  public Barrier(Type type, JobRecord jobRecord) {
    this(type, jobRecord.getRootJobKey(), jobRecord.getKey(), jobRecord.getGeneratorJobKey(),
        jobRecord.getGraphGUID());
  }

  public Barrier(Entity entity) {
    super(entity);
    jobKey = entity.getKey(JOB_KEY_PROPERTY);
    type = Type.valueOf(entity.getString(TYPE_PROPERTY));
    released = entity.getBoolean(RELEASED_PROPERTY);
    waitingOnKeys = getListProperty(WAITING_ON_KEYS_PROPERTY, entity);
    waitingOnGroupSizes = getListProperty(WAITING_ON_GROUP_SIZES_PROPERTY, entity);
  }

  @Override
  public Entity toEntity() {
    Entity.Builder entity = toProtoBuilder();
    entity.set(JOB_KEY_PROPERTY, jobKey);
    entity.set(TYPE_PROPERTY, StringValue.newBuilder(type.toString()).setExcludeFromIndexes(true).build());
    entity.set(RELEASED_PROPERTY, BooleanValue.newBuilder(released).setExcludeFromIndexes(true).build());

    ListValue.Builder waitOnKeysValue = ListValue.newBuilder();
    waitingOnKeys.stream().map(k -> KeyValue.newBuilder(k).setExcludeFromIndexes(true).build()).forEachOrdered(waitOnKeysValue::addValue);
    entity.set(WAITING_ON_KEYS_PROPERTY, waitOnKeysValue.build());

    ListValue.Builder waitingOnGroupSizesValue = ListValue.newBuilder();
    waitingOnGroupSizes.stream().map(l -> LongValue.newBuilder(l).setExcludeFromIndexes(true).build()).forEachOrdered(waitingOnGroupSizesValue::addValue);
    entity.set(WAITING_ON_GROUP_SIZES_PROPERTY, waitingOnGroupSizesValue.build());
    return entity.build();
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public void inflate(Map<Key, Slot> pool) {
    int numSlots = waitingOnKeys.size();
    waitingOnInflated = new ArrayList<>(numSlots);
    for (int i = 0; i < numSlots; i++) {
      Key key = waitingOnKeys.get(i);
      int groupSize = waitingOnGroupSizes.get(i).intValue();
      Slot slot = pool.get(key);
      if (null == slot) {
        throw new RuntimeException("No slot in pool with key=" + key);
      }
      SlotDescriptor descriptor = new SlotDescriptor(slot, groupSize);
      waitingOnInflated.add(descriptor);
    }
  }

  public void setReleased() {
    released = true;
  }

  public Object[] buildArgumentArray() {
    List<Object> argumentList = buildArgumentList();
    Object[] argumentArray = new Object[argumentList.size()];
    argumentList.toArray(argumentArray);
    return argumentArray;
  }

  public List<Object> buildArgumentList() {
    if (null == waitingOnInflated) {
      throw new RuntimeException("" + this + " has not been inflated.");
    }
    List<Object> argumentList = new LinkedList<>();
    ArrayList<Object> currentListArg = null;
    int currentListArgExpectedSize = -1;
    for (SlotDescriptor descriptor : waitingOnInflated) {
      if (!descriptor.slot.isFilled()) {
        throw new RuntimeException("Slot is not filled: " + descriptor.slot);
      }
      Object nextValue = descriptor.slot.getValue();
      if (currentListArg != null) {
        // Assert: currentListArg.size() < currentListArgExpectedSize
        if (descriptor.groupSize != currentListArgExpectedSize + 1) {
          throw new RuntimeException("expectedGroupSize: " + currentListArgExpectedSize
              + ", groupSize: " + descriptor.groupSize + "; nextValue=" + nextValue);
        }
        currentListArg.add(nextValue);
      } else {
        if (descriptor.groupSize > 0) {
          // We are not in the midst of a list and this element indicates
          // a new list is starting. This element itself is a dummy
          // marker, its value is ignored. The list is comprised
          // of the next groupSize - 1 elements.
          currentListArgExpectedSize = descriptor.groupSize - 1;
          currentListArg = new ArrayList<>(currentListArgExpectedSize);
          argumentList.add(currentListArg);
        } else if (descriptor.groupSize == 0) {
          // We are not in the midst of a list and this element is not part of
          // a list
          argumentList.add(nextValue);
        } else {
          // We were not in the midst of a list and this element is phantom
        }
      }
      if (null != currentListArg && currentListArg.size() == currentListArgExpectedSize) {
        // We have finished with the currentListArg
        currentListArg = null;
        currentListArgExpectedSize = -1;
      }
    }
    return argumentList;
  }

  private void addSlotDescriptor(SlotDescriptor slotDescr) {
    if (null == waitingOnInflated) {
      waitingOnInflated = new LinkedList<>();
    }
    waitingOnInflated.add(slotDescr);
    waitingOnGroupSizes.add((long) slotDescr.groupSize);
    Slot slot = slotDescr.slot;
    slot.addWaiter(this);
    waitingOnKeys.add(slotDescr.slot.getKey());
  }

  public void addRegularArgumentSlot(Slot slot) {
    verifyStateBeforeAdd(slot);
    addSlotDescriptor(new SlotDescriptor(slot, 0));
  }

  public void addPhantomArgumentSlot(Slot slot) {
    verifyStateBeforeAdd(slot);
    addSlotDescriptor(new SlotDescriptor(slot, -1));
  }

  private void verifyStateBeforeAdd(Slot slot) {
    if (getType() == Type.FINALIZE && waitingOnInflated != null && !waitingOnInflated.isEmpty()) {
      throw new IllegalStateException("Trying to add a slot, " + slot +
          ", to an already populated finalized barrier: " + this);
    }
  }

  /**
   * Adds multiple slots to this barrier's waiting-on list representing a single
   * Job argument of type list.
   *
   * @param slotList A list of slots that will be added to the barrier and used
   *        as the elements of the list Job argument.
   * @throws IllegalArgumentException if intialSlot is not filled.
   */
  public void addListArgumentSlots(Slot initialSlot, List<Slot> slotList) {
    if (!initialSlot.isFilled()) {
      throw new IllegalArgumentException("initialSlot must be filled");
    }
    verifyStateBeforeAdd(initialSlot);
    int groupSize = slotList.size() + 1;
    addSlotDescriptor(new SlotDescriptor(initialSlot, groupSize));
    for (Slot slot : slotList) {
      addSlotDescriptor(new SlotDescriptor(slot, groupSize));
    }
  }

  @Override
  public String toString() {
    return "Barrier[" + getKeyName(getKey()) + ", " + type + ", released=" + released + ", "
        + jobKey.getName() + ", waitingOn="
        + StringUtils.toStringParallel(waitingOnKeys, waitingOnGroupSizes) + ", job="
        + getKeyName(getJobKey()) + ", parent="
        + getKeyName(getGeneratorJobKey()) + ", guid=" + getGraphGUID() + "]";
  }
}
