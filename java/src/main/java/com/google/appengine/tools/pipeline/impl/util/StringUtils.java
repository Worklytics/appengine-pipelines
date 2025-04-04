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

package com.google.appengine.tools.pipeline.impl.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/**
 * A Utility class for string operations.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
// TODO(user): consider depending and using guava instead.
public class StringUtils {

  public static String printStackTraceToString(Throwable t) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    t.printStackTrace(pw);
    pw.flush();
    sw.flush();
    return sw.toString();
  }

  public static String toString(Object x) {
    return x == null ? "null" : x.toString();
  }

  public static String toString(Object[] array) {
    StringBuilder builder = new StringBuilder(1024);
    builder.append('[');
    for (Object x : array) {
      builder.append(toString(x));
      builder.append(", ");
    }
    if (array.length > 0) {
      builder.setLength(builder.length() - 2);
    }
    builder.append(']');
    return builder.toString();
  }

  public static <E, F> String toStringParallel(List<E> listA, List<F> listB) {
    if (listA.size() != listB.size()) {
      throw new IllegalArgumentException("The two lists must have the same length.");
    }
    StringBuilder builder = new StringBuilder(1024);
    builder.append('<');
    int i = 0;
    for (E x : listA) {
      F y = listB.get(i++);
      if (i > 1) {
        builder.append(", ");
      }
      builder.append('(').append(toString(x)).append(',').append(toString(y)).append(')');
    }
    builder.append('>');
    return builder.toString();
  }
}
