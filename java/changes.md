## Change Log

### 0.3+worklytics.7 (March 2025)
 - Handle cross-services transactionality
 - Fixes: serialization

### 0.3+worklytics.2
 - all job/promise handles format has changed; should not be used with any existing jobs
 - dependency on GAE SDK's Datastore client removed
 - you can now configure Datastore to be used to back pipeline, by specifying a project/service account

Planned:
 - remove GAE Tasks client dependency
 - remove rest of GAE SDK dependencies (module service, SystemProperty, etc)

### v0.10
  - migrates to java8, guava 20+
  - adds lombok, mockito
  
  
