# App Engine Pipeline Framework for Java


## Continuous Integration
[![Codeship Status for Worklytics/appengine-pipelines](https://app.codeship.com/projects/341fae40-195c-0137-b96c-1a1a0859fc7b/status?branch=master)](https://app.codeship.com/projects/328456)


## Change Log

### v0.4
  - migrates to java8, guava 20+
  - drop support for `onBackend` job settings; migrate `onModule`/`moduleVersion` to `onService`/`onServiceVersion` (eg, reflect current GAE branding of this functionality)
  - policy for retries against Cloud Datastore / Cloud Tasks APIs has changed slightly, so behavior under failure conditions may vary from prior versions
  

## Building

### Maven
Run tests:
```bash
mvn test
``` 

Package:
```bash
mvn package
```

Your JAR will be in `java/target/`
