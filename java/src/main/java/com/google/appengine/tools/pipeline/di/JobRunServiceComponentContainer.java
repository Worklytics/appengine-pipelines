package com.google.appengine.tools.pipeline.di;



public final class JobRunServiceComponentContainer {
  private JobRunServiceComponentContainer() {}

  // suggested by chat-gpt to get static thread-safe initialization with min synchronization overhead
  private static class Holder {
    private static final JobRunServiceComponent INSTANCE =
      DaggerJobRunServiceComponent.create();
  }

  public static JobRunServiceComponent getInstance() {
    return Holder.INSTANCE;
  }
}