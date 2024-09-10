package com.google.appengine.tools.pipeline.di;

import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import dagger.Subcomponent;

/**
 * Dagger component to be used by per-tenant clients that are using pipelines, mainly via PipelineService interface
 */
@TenantScoped
@Subcomponent(
  modules = {
    TenantModule.class,
  }
)
public interface TenantComponent {

  PipelineService pipelineService();

  PipelineManager pipelineManager();
}
