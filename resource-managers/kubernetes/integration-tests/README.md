---
layout: global
title: Spark on Kubernetes Integration Tests
---

# Running the Kubernetes Integration Tests

Note that the integration test framework is currently being heavily revised and
is subject to change. Note that currently the integration tests only run with Java 8.

The simplest way to run the integration tests is to install and run Minikube, then run the following:

    dev/dev-run-integration-tests.sh

The minimum tested version of Minikube is 0.23.0. The kube-dns addon must be enabled. Minikube should
run with a minimum of 3 CPUs and 4G of memory:

    minikube start --cpus 3 --memory 4096

You can download Minikube [here](https://github.com/kubernetes/minikube/releases).

# Integration test customization

Configuration of the integration test runtime is done through passing different arguments to the test script. The main useful options are outlined below.

## Use a non-local cluster

To use your own cluster running in the cloud, set the following:

* `--deploy-mode cloud` to indicate that the test is connecting to a remote cluster instead of Minikube,
* `--spark-master <master-url>` - set `<master-url>` to the externally accessible Kubernetes cluster URL,
* `--image-repo <repo>` - set `<repo>` to a write-accessible Docker image repository that provides the images for your cluster. The framework assumes your local Docker client can push to this repository.

Therefore the command looks like this:

    dev/dev-run-integration-tests.sh \
      --deploy-mode cloud \
      --spark-master https://example.com:8443/apiserver \
      --image-repo docker.example.com/spark-images

## Re-using Docker Images

By default, the test framework will build new Docker images on every test execution. A unique image tag is generated,
and it is written to file at `target/imageTag.txt`. To reuse the images built in a previous run, or to use a Docker image tag
that you have built by other means already, pass the tag to the test script:

    dev/dev-run-integration-tests.sh --image-tag <tag>

where if you still want to use images that were built before by the test framework:

    dev/dev-run-integration-tests.sh --image-tag $(cat target/imageTag.txt)

## Customizing the Spark Source Code to Test

By default, the test framework will test the master branch of Spark from [here](https://github.com/apache/spark). You
can specify the following options to test against different source versions of Spark:

* `--spark-repo <repo>` - set `<repo>` to the git or http URI of the Spark git repository to clone
* `--spark-branch <branch>` - set `<branch>` to the branch of the repository to build.


An example:

    dev/dev-run-integration-tests.sh \
      --spark-repo https://github.com/apache-spark-on-k8s/spark \
      --spark-branch new-feature

Additionally, you can use a pre-built Spark distribution. In this case, the repository is not cloned at all, and no
source code has to be compiled.

* `--spark-tgz <path-to-tgz>` - set `<path-to-tgz>` to point to a tarball containing the Spark distribution to test.

When the tests are cloning a repository and building it, the Spark distribution is placed in `target/spark/spark-<VERSION>.tgz`.
Reuse this tarball to save a significant amount of time if you are iterating on the development of these integration tests.

## Customizing the Namespace and Service Account

* `--namespace <namespace>` - set `<namespace>` to the namespace in which the tests should be run.
* `--service-account <service account name>` - set `<service account name>` to the name of the Kubernetes service account to
use in the namespace specified by the `--namespace`. The service account is expected to have permissions to get, list, watch,
and create pods. For clusters with RBAC turned on, it's important that the right permissions are granted to the service account
in the namespace through an appropriate role and role binding. A reference RBAC configuration is provided in `dev/spark-rbac.yaml`.