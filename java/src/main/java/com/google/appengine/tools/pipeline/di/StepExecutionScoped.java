package com.google.appengine.tools.pipeline.di;

import javax.inject.Scope;

/**
 * objects marked with this scope will live for lifetime of a step-execution
 *
 *  (Eg, executing a single "step" within a job pipeline; analogous to a request if this is via
 *   a web service run ... but in principle might not be)
 *
 * eg, everything that's scoped to the job instance, and not safe to be shared between jobs or
 * different instances of same job, should get this.
 *
 * @see StepExecutionComponent
 */
@Scope
public @interface StepExecutionScoped {
}
