# Customer-Managed Encryption Keys (CMEK) Support

## Overview
This design document describes the support for encrypting potentially sensitive job parameters using Customer-Managed Encryption Keys (CMEK) via Google Cloud KMS within the pipeline framework.

The primary use case is to ensure that sensitive job parameters are encrypted with customer-specific keys, even when the pipelines are running in shared task queues or when arbitrary content for the task is stored in the datastore records for the job/pipeline.

## Design

### Job Settings

A new job setting called `EncryptionKey` has been added. It is a `StringValuedSetting` that holds the full Google Cloud KMS Key Name. 
When creating a pipeline or a job, developers can pass the `EncryptionKey` setting:

```java
JobSetting[] settings = new JobSetting[] {
    new JobSetting.EncryptionKey("projects/my-project/locations/global/keyRings/my-keyring/cryptoKeys/my-key")
};
```

This setting is stored within the JobRecord's `QueueSettings` and passed along to any spawned pipeline tasks.

### Encryption at Enqueue Time

When a job creates tasks that need to be enqueued (via `PipelineTask.toTaskSpec()`), it checks if an `EncryptionKey` is present in the `QueueSettings`. 

If present, the framework:
1. Translates the task's properties into a JSON object.
2. Encrypts the JSON representation using the configured GCP KMS key using a utility class (`CmekUtils`).
3. Base64 encodes the ciphertext.
4. Uses a single parameter `_encrypted_payload` to hold the encrypted Base64 string for the `POST` request payload.
5. Adds an HTTP Header `X-Pipeline-EncryptionKey` carrying the KMS key name so the receiver knows which key to use for decryption.

### Decryption at Task Execution Time

When the task is received by `TaskHandler.java`:
1. It checks for the `X-Pipeline-EncryptionKey` header.
2. If the header and the `_encrypted_payload` parameter exist, it decrypts the Base64 decoded payload using the specified key via `CmekUtils`.
3. The resulting decrypted JSON is then parsed back into standard task parameters and the pipeline framework execution proceeds normally without any need to alter internal logic.

## Datastore Consideration

The current CMEK support directly encrypts the task payload (POST parameters) going into Cloud Tasks or App Engine Task Queues.
For encrypting parameters saved to the Datastore, developers can implement an `EncryptionSerializationStrategy` or explicitly encrypt their `Value`s before feeding them into the pipeline, to ensure data stored at rest in the Datastore uses CMEK. The current explicit encryption guarantees that any data placed in the Task Queues is securely encrypted under the provided CMEK.

## Dependencies

The implementation binds to the `google-cloud-kms` library to perform KMS operations.
