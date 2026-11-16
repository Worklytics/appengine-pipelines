package com.google.appengine.tools.pipeline.di;

/**
 * The process' single {@link MultiTenantComponent}: its singletons (eg, AppEngineServicesService, with its caches)
 * are meant to be shared by every tenant, rather than rebuilt for each PipelineService/PipelineManager instance.
 */
public final class MultiTenantComponentContainer {
  private MultiTenantComponentContainer() {}

  private static class Holder {
    private static final MultiTenantComponent INSTANCE = DaggerMultiTenantComponent.create();
  }

  public static MultiTenantComponent getInstance() {
    return Holder.INSTANCE;
  }
}
