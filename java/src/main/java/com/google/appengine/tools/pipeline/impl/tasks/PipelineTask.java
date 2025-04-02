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

import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineServicesService;
import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import lombok.Getter;
import lombok.NonNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * A Pipeline Framework task to be executed asynchronously This is the abstract base class for all
 * Pipeline task types.
 *
 * q: kinda analogous to a StepExecution in Spring Batch?
 *  --> yeah, think so; not *really* coupled to GAE Task queue ... that's more of implementation detail of runner
 *
 * <p>
 * This class represents both a task to be enqueued and a task being handled.
 * <p>
 * When enqueueing a task, construct a concrete subclass with the appropriate
 * data, and then add the task to an
 * {@link com.google.appengine.tools.pipeline.impl.backend.UpdateSpec} and
 * {@link com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd#save save}.
 * Alternatively the task may be enqueued directly using
 * {@link com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd#enqueue(PipelineTask)}.
 * <p>
 * When handling a task, construct a {@link Properties} object containing the
 * relevant parameters from the request, its name and then invoke
 * {@link #fromProperties(String, Properties)}.
 *
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public abstract class PipelineTask {

  protected static final String TASK_TYPE_PARAMETER = "taskType";

  @Getter @NonNull
  private final Type type;
  @Getter // nullable, if not a deterministically 'named' task (will get a name upon enqueue)
  private final String taskName;
  @Getter @NonNull
  private final QueueSettings queueSettings;

  private enum TaskProperty {

    ON_SERVICE {
      @Override
      void setProperty(PipelineTask pipelineTask, String value) {
        pipelineTask.getQueueSettings().setOnService(value);
      }

      @Override
      String getProperty(PipelineTask pipelineTask) {
        return pipelineTask.getQueueSettings().getOnService();
      }
    },
    ON_SERVICE_VERSION {
      @Override
      void setProperty(PipelineTask pipelineTask, String value) {
        pipelineTask.getQueueSettings().setOnServiceVersion(value);
      }

      @Override
      String getProperty(PipelineTask pipelineTask) {
        return pipelineTask.getQueueSettings().getOnServiceVersion();
      }
    },
    ON_QUEUE {
      @Override
      void setProperty(PipelineTask pipelineTask, String value) {
        pipelineTask.getQueueSettings().setOnQueue(value);
      }

      @Override
      String getProperty(PipelineTask pipelineTask) {
        return pipelineTask.getQueueSettings().getOnQueue();
      }
    },
    DELAY {
      @Override
      void setProperty(PipelineTask pipelineTask, String value) {
        if (value != null) {
          pipelineTask.getQueueSettings().setDelayInSeconds(Long.parseLong(value));
        }
      }

      @Override
      String getProperty(PipelineTask pipelineTask) {
        Long delay = pipelineTask.getQueueSettings().getDelayInSeconds();
        return delay == null ? null : delay.toString();
      }
    };

    static final Set<TaskProperty> ALL = EnumSet.allOf(TaskProperty.class);

    abstract void setProperty(PipelineTask pipelineTask, String value);
    abstract String getProperty(PipelineTask pipelineTask);

    void applyFrom(PipelineTask pipelineTask, Properties properties) {
      String value = properties.getProperty(name());
      if (value != null) {
        setProperty(pipelineTask, value);
      }
    }

    void addTo(PipelineTask pipelineTask, Properties properties) {
      String value = getProperty(pipelineTask);
      if (value != null) {
        properties.setProperty(name(), value);
      }
    }
  }

  /**
   * The type of task. The Pipeline framework uses several types
   */
  public enum Type {

    HANDLE_SLOT_FILLED(HandleSlotFilledTask.class),
    RUN_JOB(RunJobTask.class),
    HANDLE_CHILD_EXCEPTION(HandleChildExceptionTask.class),
    CANCEL_JOB(CancelJobTask.class),
    FINALIZE_JOB(FinalizeJobTask.class),
    DELETE_PIPELINE(DeletePipelineTask.class),
    DELAYED_SLOT_FILL(DelayedSlotFillTask.class),
    ;

    private final Constructor<? extends PipelineTask> taskConstructor;

    Type(Class<? extends PipelineTask> taskClass) {
      try {
        taskConstructor = taskClass.getDeclaredConstructor(
            getClass(), String.class, Properties.class);
        taskConstructor.setAccessible(true);
      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Invalid Task class " + taskClass, e);
      }
    }

    public PipelineTask createInstance(String taskName, Properties properties) {
      try {
        return taskConstructor.newInstance(this, taskName, properties);
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
          | InvocationTargetException e) {
        throw new RuntimeException("Unexpected exception while creating new instance for "
            + taskConstructor.getDeclaringClass(), e);
      }
    }
  }

  /**
   * This constructor is used on the sending side. That is, it is used to
   * construct a task to be enqueued.
   */
  protected PipelineTask(@NonNull Type type, String taskName, @NonNull QueueSettings queueSettings) {
    this.type = type;
    this.taskName = taskName;
    this.queueSettings = queueSettings;
  }

  protected PipelineTask(Type type, String taskName, Properties properties) {
    this(type, taskName, new QueueSettings());
    for (TaskProperty taskProperty : TaskProperty.ALL) {
      taskProperty.applyFrom(this, properties);
    }
  }



  public final Properties toProperties() {
    Properties properties = new Properties();
    properties.setProperty(TASK_TYPE_PARAMETER, type.toString());
    for (TaskProperty taskProperty : TaskProperty.ALL) {
      taskProperty.addTo(this, properties);
    }
    addProperties(properties);
    return properties;
  }


  public PipelineTaskQueue.TaskSpec toTaskSpec(AppEngineServicesService appEngineServicesService, String callback) {
    PipelineTaskQueue.TaskSpec.TaskSpecBuilder spec = PipelineTaskQueue.TaskSpec.builder()
      .name(this.getTaskName())
      .callbackPath(callback)
      .method(PipelineTaskQueue.TaskSpec.Method.POST);

    this.toProperties().entrySet()
      .forEach(p -> spec.param((String) p.getKey(), (String) p.getValue()));

    if (this.getQueueSettings().getDelayInSeconds() != null) {
      spec.scheduledExecutionTime(Instant.now().plusSeconds(this.getQueueSettings().getDelayInSeconds()));
    }

    String service = Optional.ofNullable(this.getQueueSettings().getOnService())
      .orElseGet(appEngineServicesService::getDefaultService);
    spec.service(service);

    String version = this.getQueueSettings().getOnServiceVersion();
    spec.version(version);

    return spec.build();
  }

  /**
   * Construct a task from {@code Properties}. This method is used on the
   * receiving side. That is, it is used to construct a {@code Task} from an
   * HttpRequest sent from the App Engine task queue. {@code properties} must
   * contain a property named {@link #TASK_TYPE_PARAMETER}. In addition it must
   * contain the properties specified by the concrete subclass of this class
   * corresponding to the task type.
   */
  public static PipelineTask fromProperties(String taskName, @NonNull Properties properties) {
    String taskTypeString = properties.getProperty(TASK_TYPE_PARAMETER);
    if (null == taskTypeString) {
      throw new IllegalArgumentException(TASK_TYPE_PARAMETER + " property is missing: "  + properties);
    }
    Type type = Type.valueOf(taskTypeString);
    return type.createInstance(taskName, properties);
  }

  @Override
  public String toString() {
    String value = getType() + "_TASK[name=" + getTaskName() + ", queueSettings=" + getQueueSettings();
    String extraProperties = propertiesAsString();
    if (extraProperties != null && !extraProperties.isEmpty()) {
      value += ", " + extraProperties;
    }
    return value + "]";
  }

  protected abstract String propertiesAsString();

  protected abstract void addProperties(Properties properties);
}
