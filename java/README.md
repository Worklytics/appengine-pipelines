# App Engine Pipeline Framework for Java

## Usage

Worklytics's version of this framework is published in our [GitHub Package repository](https://github.com/Worklytics
/appengine-pipelines/packages).
 
  1. Ensure you authenticate Maven such that it can obtain packages from GitHub. See [Authenticating to GitHub
   Packages](https://help.github.com/en/github/managing-packages-with-github-packages/configuring-apache-maven-for-use-with-github-packages#authenticating-to-github-packages).
  2. Include our GitHub package repository in your `pom.xml`:
```xml
   <repositories>
     <repository>
       <id>github</id>
       <name>Apache Maven Packages by Worklytics</name>
       <url>https://maven.pkg.github.com/worklytics/packages</url>
     </repository>
   </repositories>
```
  3. Specifically include our package in your `pom.xml`:
```xml
<dependency>
  <groupId>com.google.appengine.tools</groupId>
  <artifactId>appengine-pipeline</artifactId>
  <version>0.3-SNAPSHOT</version>
</dependency>
```

Please review [changes.md](changes.md) for highlights of major changes between each of our builds.

## Build Instructions

Only Maven is currently supported.

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

### Development

  - `USE_LEGACY_QUEUES` - set to use task queues via legacy GAE SDK instead of Cloud Tasks API client; useful for local development, to use locally emulated queues + GAE
  - `USE_LOCAL_SERVICE` - to assume that this and all services is running as `default`/`v1` on `localhost`; atm, used for setting target of enqueued tasks
  - `GAE_SERVICE_HOST_SUFFIX` - if provided, used instead of doing AppEngine Admin API lookup to determine the service host suffix; in practice, this doesn't change so no point in doing API calls
  - `CLOUDTASKS_QUEUE_LOCATION` - if provided, used instead of doing lookup against AppEngine Admin API to determine queue locations; useful bc always known at deployment time, so no need to lookup dynamically
