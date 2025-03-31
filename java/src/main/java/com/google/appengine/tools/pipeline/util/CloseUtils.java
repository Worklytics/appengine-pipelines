package com.google.appengine.tools.pipeline.util;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

public class CloseUtils {

  public static void closeQuietly(@Nullable Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  public static void closeQuietly(@Nullable AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }

}
