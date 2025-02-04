/**
 * Copyright 2024, Worklytics, Co.
 *
 */
package com.google.appengine.tools.pipeline.impl.util;

import com.google.common.annotations.VisibleForTesting;
import lombok.SneakyThrows;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.*;

/**
 * worklytics version of this class replaces Google version w same public interface/behavior, but replacement offers
 * much better performance, exploits modern java behavior; eliminates need to measure headers ourselves, etc.
 *
 * q: use JSON serialization? more standard and potentially interoperable across languages and java versions
 */
public class SerializationUtils {

  // err on small; test >= this actually runs faster than < this, suggesting efficiency handling compressed data
  @VisibleForTesting
  static final int MAX_UNCOMPRESSED_BYTE_SIZE = 50_000;

  public static byte[] serialize(Object obj) throws IOException {
    // Serialize the object
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOut = new ObjectOutputStream(byteOut)) {
      objectOut.writeObject(obj);
    }
    // Compress only if serialized data exceeds threshold
    if (byteOut.size() > MAX_UNCOMPRESSED_BYTE_SIZE) {
      ByteArrayOutputStream compressedByteOut = new ByteArrayOutputStream();
      try (GZIPOutputStream gzipOut = new GZIPOutputStream(compressedByteOut);
           ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut)) {
        objectOut.writeObject(obj);
        objectOut.flush();
        gzipOut.finish();
        return compressedByteOut.toByteArray();
      }
    }
    return byteOut.toByteArray();
  }

  @SneakyThrows
  public static byte[] serializeToByteArray(Serializable o) {
    return SerializationUtils.serialize(o);
  }


  public static <T> T deserialize(byte[] data) throws IOException, ClassNotFoundException {
    // Attempt to decompress
    try (ByteArrayInputStream byteIn = new ByteArrayInputStream(data)) {
      if (isGZIPCompressed(data)) {
        try (GZIPInputStream gzipIn = new GZIPInputStream(byteIn);
             ObjectInputStream objectIn = new ObjectInputStream(gzipIn)) {
          return (T) objectIn.readObject();
        }
      } else {
        try (ObjectInputStream objectIn = new ObjectInputStream(byteIn)) {
          return (T) objectIn.readObject();
        }
      }
    }
  }

  @VisibleForTesting
  static boolean isGZIPCompressed(byte[] bytes) {
    return (bytes != null)
      && (bytes.length >= 2)
      && ((bytes[0] == (byte) (GZIPInputStream.GZIP_MAGIC))
      && (bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8)));
  }


  public static byte[] getBytes(ByteBuffer in) {
    if (in.hasArray() && in.position() == 0
      && in.arrayOffset() == 0 && in.array().length == in.limit()) {
      return in.array();
    } else {
      byte[] buf = new byte[in.remaining()];
      int position = in.position();
      in.get(buf);
      in.position(position);
      return buf;
    }
  }

  @SneakyThrows
  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T clone(T toClone) {
    byte[] bytes = serializeToByteArray(toClone);
    return deserialize(bytes);
  }

}
