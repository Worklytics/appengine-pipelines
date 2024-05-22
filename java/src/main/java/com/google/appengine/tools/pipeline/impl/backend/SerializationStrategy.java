package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.model.PipelineModelObject;

import java.io.IOException;

public interface SerializationStrategy {

  /**
   * Given an arbitrary Java Object, returns another object that encodes the
   * given object but that is guaranteed to be of a type supported by the App
   * Engine Data Store. Use
   * {@link #deserializeValue(PipelineModelObject, Object)} to reverse this
   * operation.
   *
   * @param model The model that is associated with the value.
   * @param value An arbitrary Java object to serialize.
   * @return The serialized version of the object.
   * @throws IOException if any problem occurs
   */
  Object serializeValue(PipelineModelObject model, Object value) throws IOException;

  /**
   * Reverses the operation performed by
   * {@link #serializeValue(PipelineModelObject, Object)}.
   *
   * @param model The model that is associated with the serialized version.
   * @param serializedVersion The serialized version of an object.
   * @return The deserialized version of the object.
   * @throws IOException if any problem occurs
   */
  Object deserializeValue(PipelineModelObject model, Object serializedVersion)
    throws IOException, ClassNotFoundException;

}
