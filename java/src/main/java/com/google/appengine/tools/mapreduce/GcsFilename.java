package com.google.appengine.tools.mapreduce;

import com.google.cloud.storage.BlobId;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * container for a GcsFilename
 *
 * dropin replacement for com.google.appengine.tools.cloudstorage.GcsFilename
 *
 * @see com.google.appengine.tools.cloudstorage.GcsFilename
 */
@Getter
@RequiredArgsConstructor
@ToString
public final class GcsFilename implements Serializable {
  private static final long serialVersionUID = 1L;

  @NonNull
  private final String bucketName;

  @NonNull
  private final String objectName;

  public BlobId asBlobId() {
    return BlobId.of(getBucketName(), getObjectName());
  }

}
