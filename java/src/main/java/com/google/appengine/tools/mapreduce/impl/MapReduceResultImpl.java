// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import lombok.NoArgsConstructor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * Implementation of {@link MapReduceResult}.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <R> type of result
 */
@NoArgsConstructor
public class MapReduceResultImpl<R> implements MapReduceResult<R>, Externalizable {

  private static final long serialVersionUID = 237070477689138395L;

  private R outputResult; // can be null
  private Counters counters;

  public MapReduceResultImpl(R outputResult, Counters counters) {
    if (outputResult != null) {
      checkArgument(outputResult instanceof Serializable, "outputResult(%s) should be serializable",
          outputResult.getClass());
    }
    this.outputResult = outputResult;
    this.counters = checkNotNull(counters, "Null counters");
  }

  @Override
  public R getOutputResult() {
    return outputResult;
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + outputResult + ", "
        + counters + ")";
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(counters);
    if (outputResult != null) {
      byte[] bytes = SerializationUtil.serialize((Serializable) outputResult);
      out.writeObject(bytes);
    } else {
      out.writeObject(null);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    counters = (Counters) in.readObject();
    byte[] bytes = (byte[]) in.readObject();
    if (bytes != null) {
      outputResult = (R) SerializationUtil.deserialize(bytes);
    }
  }
}
