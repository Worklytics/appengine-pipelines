/**
 * Copyright 2024, Worklytics, Co.
 *
 */
package com.google.appengine.tools.pipeline.impl.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.*;

/**
 * worklytics version of this class replaces Google version w same public interface/behavior, but replacment offers
 * much better performance, exploits modern java behavior; eliminates need to measure headers ourselves, etc.
 */
public class SerializationUtils {

  private static final int MAX_UNCOMPRESSED_BYTE_SIZE = 1_000_000;

  public static byte[] serialize(Object obj) throws IOException {
    // Serialize the object
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOut = new ObjectOutputStream(byteOut)) {
      objectOut.writeObject(obj);
    }
    byte[] serializedData = byteOut.toByteArray();

    // Compress only if serialized data exceeds threshold
    if (serializedData.length > MAX_UNCOMPRESSED_BYTE_SIZE) {
      ByteArrayOutputStream compressedByteOut = new ByteArrayOutputStream();
      try (GZIPOutputStream gzipOut = new GZIPOutputStream(compressedByteOut);
           ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut)) {
        objectOut.writeObject(obj);
        objectOut.flush();
        gzipOut.finish();
        serializedData = compressedByteOut.toByteArray();
      }
    }
    return serializedData;
  }

  public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
    // Attempt to decompress
    try (ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
         GZIPInputStream gzipIn = new GZIPInputStream(byteIn);
         ObjectInputStream objectIn = new ObjectInputStream(gzipIn)) {
      return objectIn.readObject();
    } catch (IOException e) {
      // If decompression fails, assume data was not compressed
      try (ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
           ObjectInputStream objectIn = new ObjectInputStream(byteIn)) {
        return objectIn.readObject();
      }
    }
  }
}
