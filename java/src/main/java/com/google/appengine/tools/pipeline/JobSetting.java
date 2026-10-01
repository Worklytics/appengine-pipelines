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

package com.google.appengine.tools.pipeline;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A setting for specifying to the framework some aspect of a Job's execution.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface JobSetting extends Serializable {

  /**
   * A setting for specifying that a Job should not be run until the value slot
   * represented by the given {@code FutureValue} has been filled.
   */
  final class WaitForSetting implements JobSetting {

    @Serial
    private static final long serialVersionUID = 1961952679964049657L;
    private final Value<?> futureValue;

    public WaitForSetting(Value<?> value) {
      futureValue = value;
    }

    public Value<?> getValue() {
      return futureValue;
    }
  }

  /**
   * An abstract parent object for integer settings.
   */
  @RequiredArgsConstructor
  @Getter
  abstract class IntValuedSetting implements JobSetting {

    @Serial
    private static final long serialVersionUID = -4853437803222515955L;
    private final int value;

    @Override
    public String toString() {
      return getClass() + "[" + value + "]";
    }
  }

  /**
   * An abstract parent object for String settings.
   */
  @Getter
  @RequiredArgsConstructor
  abstract class StringValuedSetting implements JobSetting {

    @Serial
    private static final long serialVersionUID = 7756646651569386669L;

    // NOTE: behavior of Pipeline Framework allows this to be null for some settings
    // (tests verify this)
    private final String value;

    @Override
    public String toString() {
      return getClass() + "[" + value + "]";
    }
  }

  /**
   * A setting for specifying how long to wait before retrying a failed job. The
   * wait time will be
   *
   * <pre>
   * <code>
   * backoffSeconds * backoffFactor ^ attemptNumber
   * </code>
   * </pre>
   *
   */
  final class BackoffSeconds extends IntValuedSetting {

    @Serial
    private static final long serialVersionUID = -8900842071483349275L;
    public static final int DEFAULT = 15;

    public BackoffSeconds(int seconds) {
      super(seconds);
    }
  }

  /**
   * A setting for specifying how long to wait before retrying a failed job. The
   * wait time will be
   *
   * <pre>
   * <code>
   * backoffSeconds * backoffFactor ^ attemptNumber
   * </code>
   * </pre>
   *
   */
  final class BackoffFactor extends IntValuedSetting {

    @Serial
    private static final long serialVersionUID = 5879098639819720213L;
    public static final int DEFAULT = 2;

    public BackoffFactor(int factor) {
      super(factor);
    }
  }

  /**
   * A setting for specifying how many times to retry a failed job.
   */
  final class MaxAttempts extends IntValuedSetting {

    @Serial
    private static final long serialVersionUID = 8389745591294068656L;
    public static final int DEFAULT = 3;

    public MaxAttempts(int attempts) {
      super(attempts);
    }
  }

  /**
   * A setting for specifying what backend to run a job on.
   */
  @Deprecated
  final class OnBackend extends StringValuedSetting {

    private static final long serialVersionUID = -239968568113511744L;
    public static final String DEFAULT = null;

    public OnBackend(String backend) {
      super(backend);
    }
  }

  /**
   * A setting for specifying what service (module) to run a job on.
   */
  final class OnService extends StringValuedSetting {

    @Serial
    private static final long serialVersionUID = 3877411731586475273L;

    public OnService(String service) {
      super(service);
    }
  }

  /**
   * A setting for specifying what version of service (module) to run a job on.
   *
   * q: good idea to expose this? perhaps should keep internal to FW
   */
  final class OnServiceVersion extends StringValuedSetting {

    @Serial
    private static final long serialVersionUID = 3877411731586475273L;

    public OnServiceVersion(String version) {
      super(version);
    }
  }

  /**
   * A setting for specifying which queue to run a job on.
   */
  final class OnQueue extends StringValuedSetting {

    @Serial
    private static final long serialVersionUID = -5010485721032395432L;

    public OnQueue(String queue) {
      super(queue);
    }
  }

  /**
   * A setting specifying the job's status console URL.
   */
  final class StatusConsoleUrl extends StringValuedSetting {

    @Serial
    private static final long serialVersionUID = -3079475300434663590L;

    public StatusConsoleUrl(String statusConsoleUrl) {
      super(statusConsoleUrl);
    }
  }

  /**
   * A setting for specifying the datastore database to use for this pipeline.
   * Null, empty, and {@code (default)} select the default database.
   *
   * A pipeline is stored in one database and one namespace. Child jobs inherit that
   * partition and cannot select a different one. To cross partitions, create a promise
   * in the parent pipeline and pass its handle to a separate pipeline, which submits
   * the value back into the parent's database and namespace.
   */
  final class DatastoreDatabase extends StringValuedSetting {
    @Serial
    private static final long serialVersionUID = -1L;

    public static final String DEFAULT_DATABASE_ID = "(default)";

    public DatastoreDatabase(String datastoreDatabase) {
      super(datastoreDatabase);
      if (datastoreDatabase == null || datastoreDatabase.isEmpty()
          || DEFAULT_DATABASE_ID.equals(datastoreDatabase)) {
        return;
      }
      // Firestore database IDs are 4–63 characters: a letter, then letters, digits,
      // or hyphens, and must not end with a hyphen.
      if (!datastoreDatabase.matches("^[a-z][a-z0-9-]{2,61}[a-z0-9]$")) {
        throw new IllegalArgumentException("Invalid Datastore database ID: " + datastoreDatabase);
      }
    }
  }

  /**
   * A setting for specifying the datastore namespace for a pipeline. Null or empty selects the
   * default namespace; when omitted for a root job, the backend namespace is inherited. Child jobs
   * inherit the pipeline namespace and cannot select a different one.
   */
  final class DatastoreNamespace extends StringValuedSetting {
    @Serial
    private static final long serialVersionUID = -1L;

    public DatastoreNamespace(String datastoreNameSpace) {
      super(datastoreNameSpace);
      if (datastoreNameSpace != null) {
        if (!datastoreNameSpace.matches("^(?!__.*__$)[0-9A-Za-z._-]{0,100}$")) {
          throw new IllegalArgumentException("Invalid Datastore namespace: " + datastoreNameSpace);
        }
      }
    }
  }

  static <E extends StringValuedSetting> Optional<String> getSettingValue(Class<E> clazz, JobSetting[] settings) {
    return findSetting(clazz, settings).map(StringValuedSetting::getValue);
  }

  /**
   * The setting object, including one whose value is null. {@link #getSettingValue} drops null
   * values, so it cannot tell an omitted setting from an explicit default.
   */
  static <E extends JobSetting> Optional<E> findSetting(Class<E> clazz, JobSetting[] settings) {
    if (settings == null) {
      return Optional.empty();
    }
    return Arrays.stream(settings).filter(clazz::isInstance).map(clazz::cast).findAny();
  }

  /**
   * Null, blank, and {@code (default)} all mean the default Firestore database.
   * Named database IDs are returned unchanged.
   */
  public static String canonicalDatabaseId(String databaseId) {
    if (databaseId == null || databaseId.isEmpty() || DatastoreDatabase.DEFAULT_DATABASE_ID.equals(databaseId)) {
      return null;
    }
    return databaseId;
  }

  /**
   * Null and empty both mean the default namespace.
   */
  public static String canonicalNamespace(String namespace) {
    if (namespace == null || namespace.isEmpty()) {
      return null;
    }
    return namespace;
  }

  public static boolean sameDatabase(String left, String right) {
    return Objects.equals(canonicalDatabaseId(left), canonicalDatabaseId(right));
  }

  public static boolean sameNamespace(String left, String right) {
    return Objects.equals(canonicalNamespace(left), canonicalNamespace(right));
  }
}
