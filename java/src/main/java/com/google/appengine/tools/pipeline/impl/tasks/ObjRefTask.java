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

package com.google.appengine.tools.pipeline.impl.tasks;

import com.google.cloud.datastore.Key;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A subclass of {@code Task} for tasks which need to reference a particular
 * Pipeline model object. The task contains the data store key of the reference
 * object in its {@link #KEY_PARAM} property.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
@ToString
public abstract class ObjRefTask extends PipelineTask {

  private static final String KEY_PARAM = "key";

  /**
   * The {@code Key} of the object to which this {@code ObjRefTask} refers.
   */
  private final Key key;

  /**
   * This constructor is used on the sending side. That is, it is used to
   * construct a task to be enqueued.
   *
   * @param type The type of task being constructed
   * @param namePrefix for the task name.
   * @param key The {@code Key} of the object to which this {@code ObjRefTask}
   *        will refer. It will be used as part of the task name if
   *        combined with {@code namePrefix}.
   */
  protected ObjRefTask(Type type, @NonNull String namePrefix, @NonNull Key key, @NonNull QueueSettings queueSettings) {
    super(type, createTaskName(namePrefix, key), queueSettings.clone());
    this.key = key;
  }

  private static String createTaskName(@NonNull String namePrefix, @NonNull Key key) {
    //deterministic name based on key, that is legal for Task Queues
    return Stream.of(
        key.getDatabaseId(),
        key.getNamespace(),
        namePrefix,
        DigestUtils.md5Hex(key.getKind() + key.getNameOrId())
      )
      .filter(s -> s != null && !s.isEmpty())
      .collect(Collectors.joining("_"));
  }

  /**
   * This constructor is used on the receiving side. That is, it is used to
   * construct an {@code ObjRefTask} from an HttpRequest sent from the App
   * Engine task queue.
   *
   * @param type The type of task being constructed
   * @param properties In addition to the properties specified in the parent
   *        constructor, {@code properties} must also contain a property named "key"
   *        with a value of a stringified data store key. This will be used as
   *        the {@link #key} of the object to which this {@code ObjRefTask}
   *        refers.
   */
  protected ObjRefTask(Type type, String taskName, Properties properties) {
    super(type, taskName, properties);
    key = Key.fromUrlSafe(properties.getProperty(KEY_PARAM));
  }

  public Key getKey() {
    return key;
  }

  @Override
  protected void addProperties(@NonNull Properties properties) {
    properties.setProperty(KEY_PARAM, key.toUrlSafe());
  }

  @Override
  public String propertiesAsString() {
    return "key=" + key;
  }
}
