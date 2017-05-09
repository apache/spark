---
layout: global
title: Spark on Kubernetes Development
---

[Kubernetes](https://kubernetes.io/) is a framework for easily deploying, scaling, and managing containerized
applications. It would be useful for a user to run their Spark jobs on a Kubernetes cluster alongside their
other Kubernetes-managed applications. For more about the motivations for adding this feature, see the umbrella JIRA
ticket that tracks this project: [SPARK-18278](https://issues.apache.org/jira/browse/SPARK-18278).

This submodule is an initial implementation of allowing Kubernetes to be a
supported cluster manager for Spark, along with Mesos, Hadoop YARN, and Standalone. This document provides a summary of
important matters to keep in mind when developing this feature.

# Building Spark with Kubernetes Support

To build Spark with Kubernetes support, use the `kubernetes` profile when invoking Maven.

    git checkout branch-2.1-kubernetes
    build/mvn package -Pkubernetes -DskipTests

To build a distribution of Spark with Kubernetes support, use the `dev/make-distribution.sh` script, and add the
`kubernetes` profile as part of the build arguments. Any other build arguments can be specified as one would expect when
building Spark normally. For example, to build Spark against Hadoop 2.7 and Kubernetes:

    dev/make-distribution.sh --tgz -Phadoop-2.7 -Pkubernetes

# Kubernetes Code Modules

Below is a list of the submodules for this cluster manager and what they do.

* `core`: Implementation of the Kubernetes cluster manager support.
* `integration-tests`: Integration tests for the project.
* `docker-minimal-bundle`: Base Dockerfiles for the driver and the executors. The Dockerfiles are used for integration
  tests as well as being provided in packaged distributions of Spark.
* `integration-tests-spark-jobs`: Spark jobs that are only used in integration tests.
* `integration-tests-spark-jobs-helpers`: Dependencies for the spark jobs used in integration tests. These dependencies
  are separated out to facilitate testing the shipping of jars to drivers running on Kubernetes clusters.

# Running the Kubernetes Integration Tests

Note that the integration test framework is currently being heavily revised and is subject to change.

Note that currently the integration tests only run with Java 8.

Running any of the integration tests requires including `kubernetes-integration-tests` profile in the build command. In
order to prepare the environment for running the integration tests, the `pre-integration-test` step must be run in Maven
on the `resource-managers/kubernetes/integration-tests` module:

    build/mvn pre-integration-test -Pkubernetes -Pkubernetes-integration-tests -pl resource-managers/kubernetes/integration-tests -am -DskipTests
 
Afterwards, the integration tests can be executed with Maven or your IDE. Note that when running tests from an IDE, the
`pre-integration-test` phase must be run every time the Spark main code changes. When running tests from the
command line, the `pre-integration-test` phase should automatically be invoked if the `integration-test` phase is run.

After the above step, the integration test can be run using the following command:

```sh
build/mvn integration-test \
    -Pkubernetes -Pkubernetes-integration-tests \
    -pl resource-managers/kubernetes/integration-tests -am
```

# Running against an arbitrary cluster

In order to run against any cluster, use the following:
build/mvn integration-test \
    -Pkubernetes -Pkubernetes-integration-tests \
    -pl resource-managers/kubernetes/integration-tests -am
    -DextraScalaTestArgs="-Dspark.kubernetes.test.master=k8s://https://<master> -Dspark.docker.test.driverImage=<driver-image> -Dspark.docker.test.executorImage=<executor-image>"

# Preserve the Minikube VM

The integration tests make use of [Minikube](https://github.com/kubernetes/minikube), which fires up a virtual machine
and setup a single-node kubernetes cluster within it. By default the vm is destroyed after the tests are finished.
If you want to preserve the vm, e.g. to reduce the running time of tests during development, you can pass the property
`spark.docker.test.persistMinikube` to the test process:

```sh
build/mvn integration-test \
    -Pkubernetes -Pkubernetes-integration-tests \
    -pl resource-managers/kubernetes/integration-tests -am \
    -DextraScalaTestArgs=-Dspark.docker.test.persistMinikube=true
```

# Usage Guide

See the [usage guide](../../docs/running-on-kubernetes.md) for more information.
