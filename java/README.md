# App Engine Pipeline Framework for Java

## Usage

Worklytics's version of this framework is published in our [GitHub Package repository](https://github.com/Worklytics
/appengine-pipelines/packages).

## Change Log

### v0.4
  - migrates to java8, guava 20+
  - drop support for `onBackend` job settings; migrate `onModule`/`moduleVersion` to `onService`/`onServiceVersion` (eg, reflect current GAE branding of this functionality)
  - policy for retries against Cloud Datastore / Cloud Tasks APIs has changed slightly, so behavior under failure conditions may vary from prior versions
  

## Building

### Maven
Run tests:
```shell script
mvn test
``` 

Package:
```shell script
mvn package
```

Your JAR will be in `java/target/`

### Deployment

 1. create a GitHub personal access token and put it in your `/.m2/settings.xml`, as described in [GitHub's docs](https://help.github.com/en/github/managing-packages-with-github-package-registry/configuring-apache-maven-for-use-with-github-package-registry)
 2. run the following (from the `java/` subdirectory of the repo):
 ```shell script
mvn deploy
```
