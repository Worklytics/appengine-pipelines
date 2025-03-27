package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.pipeline.PipelineOrchestrator;
import com.google.appengine.tools.pipeline.PipelineRunner;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.PipelineManager;

import dagger.Subcomponent;

/**
 * objects marked with this scope will live for lifetime of a step-execution
 * (in our GAE fw context, this happens to be async via task queues that callback via webhooks; but that's an implementation detail)
 *
 * see {@link StepExecutionScoped}
 */
@StepExecutionScoped
@Subcomponent(
  modules = {
    StepExecutionModule.class,
  }
)
public interface StepExecutionComponent {

  PipelineRunner pipelineRunner();

  PipelineOrchestrator pipelineOrchestrator();

  PipelineManager pipelineManager();

  PipelineService pipelineService();

  ShardedJobRunner shardedJobRunner();

}
