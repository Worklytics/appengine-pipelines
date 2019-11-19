# Pipelines Framework for Google App Engine


## Current Vision

 - Orchestration of asynchronous workflows on top of Google App Engine, including sub-jobs potentially performed in
  other services
 - Support analogous concepts to Apache Beam API, but maintaining flexibility, re-use on top of GAE. 
 - Support for multi-tenant applications, which can isolate pipelines data on a per-tenant basis

## Original project

Official site of original project, which is no longer maintained by Google: https://github.com/GoogleCloudPlatform
/appengine-pipelines

The Google App Engine Pipeline API connects together complex, workflows (including human tasks). The goals are
 flexibility, workflow reuse, and testability.

A primary use-case of the API is connecting together various App Engine MapReduces into a computational pipeline.
