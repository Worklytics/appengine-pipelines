package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Context;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobId;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;


/**
 * Base class for all Context implementations.
 */
@Getter
@AllArgsConstructor
public class BaseContext implements Context, Serializable {

  //q: change this to a ShardedJobId?? why not??
  private final ShardedJobId jobId;
}