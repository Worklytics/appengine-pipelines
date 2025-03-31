package com.google.appengine.tools.mapreduce.impl.pipeline;


import com.google.appengine.tools.mapreduce.GcpCredentialOptions;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;
import com.google.cloud.storage.Storage;
import lombok.RequiredArgsConstructor;

import java.io.Serial;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A job which deletes all the files in the provided GoogleCloudStorageFileSet
 */
@RequiredArgsConstructor
public class DeleteFilesJob extends Job1<Void, List<GcsFilename>> {

  @Serial
  private static final long serialVersionUID = 4821135390816992131L;
  private static final Logger log = Logger.getLogger(DeleteFilesJob.class.getName());

  private final GcpCredentialOptions credentialOptions;


  /**
   * Deletes the files in the provided GoogleCloudStorageFileSet
   */
  @Override
  public Value<Void> run(List<GcsFilename> files) throws Exception {
    try (Storage storage = GcpCredentialOptions.getStorageClient(credentialOptions)) {
      files.stream()
        .map(GcsFilename::asBlobId)
        .forEach(blobId -> {
          boolean r = storage.delete(blobId);
          if (!r) {
            // not found? deletion failed? or access denied?
            log.log(Level.WARNING, "Failed to cleanup file: " + blobId);
          }
        });
    } catch (Throwable e) {
      log.log(Level.WARNING, "Failed to cleanup files", e);
    }

    return null;
  }
}
