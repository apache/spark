---
layout: global
title: Spark on Kubernetes Integration Tests
---

# Running the Kubernetes Integration Tests

Note that the integration test framework is currently being heavily revised and
is subject to change. Note that currently the integration tests only run with Java 8.

The simplest way to run the integration tests is to install and run Minikube, then run the following:

    dev/dev-run-integration-tests.sh

For Java 8u251+, `HTTP2_DISABLE=true` and `spark.kubernetes.driverEnv.HTTP2_DISABLE=true` are required additionally for fabric8 `kubernetes-client` library to talk to Kubernetes clusters. This prevents `KubernetesClientException` when `kubernetes-client` library uses `okhttp` library internally.

The minimum tested version of Minikube is 0.23.0. The kube-dns addon must be enabled. Minikube should
run with a minimum of 3 CPUs and 4G of memory:

    minikube start --cpus 3 --memory 4096

You can download Minikube [here](https://github.com/kubernetes/minikube/releases).

# Integration test customization

Configuration of the integration test runtime is done through passing different arguments to the test script. The main useful options are outlined below.

## Re-using Docker Images

By default, the test framework will build new Docker images on every test execution. A unique image tag is generated,
and it is written to file at `target/imageTag.txt`. To reuse the images built in a previous run, or to use a Docker image tag
that you have built by other means already, pass the tag to the test script:

    dev/dev-run-integration-tests.sh --image-tag <tag>

where if you still want to use images that were built before by the test framework:

    dev/dev-run-integration-tests.sh --image-tag $(cat target/imageTag.txt)

## Spark Distribution Under Test

The Spark code to test is handed to the integration test system via a tarball. Here is the option that is used to specify the tarball:

* `--spark-tgz <path-to-tgz>` - set `<path-to-tgz>` to point to a tarball containing the Spark distribution to test.

TODO: Don't require the packaging of the built Spark artifacts into this tarball, just read them out of the current tree.

## Customizing the Namespace and Service Account

* `--namespace <namespace>` - set `<namespace>` to the namespace in which the tests should be run.
* `--service-account <service account name>` - set `<service account name>` to the name of the Kubernetes service account to
use in the namespace specified by the `--namespace`. The service account is expected to have permissions to get, list, watch,
and create pods. For clusters with RBAC turned on, it's important that the right permissions are granted to the service account
in the namespace through an appropriate role and role binding. A reference RBAC configuration is provided in `dev/spark-rbac.yaml`.
