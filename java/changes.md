## Change Log

### v0.3
  - migrates to java8, guava 20+
  - drop support for `onBackend` job settings; migrate `onModule`/`moduleVersion` to `onService`/`onServiceVersion` (eg, reflect current GAE branding of this functionality)
  - policy for retries against Cloud Datastore / Cloud Tasks APIs has changed slightly, so behavior under failure conditions may vary from prior versions
  
  
