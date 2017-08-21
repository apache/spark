---
layout: global
title: Implementation of Submitting Applications to Kubernetes
---


Similarly to YARN and Standalone mode, it is common for Spark applications to be deployed on Kubernetes through the
`spark-submit` process. Applications are deployed on Kubernetes via sending YAML files to the Kubernetes API server.
These YAML files declare the structure and behavior of the processes that will be run. However, such a declarative
approach to application deployment differs considerably from how Spark applications are deployed via the `spark-submit`
API. There are contracts provided by `spark-submit` that should work in Kubernetes in a consistent manner to the other
cluster managers that `spark-submit` can deploy on.

This document outlines the design of the **Kubernetes submission client**, which effectively serves as a *translation
of options provided in spark-submit to a specification of one or more Kubernetes API resources that represent the
Spark driver*.

# Entry Point

As with the other cluster managers, the user's invocation of `spark-submit` will eventually delegate to running
`org.apache.spark.deploy.SparkSubmit#submit`. This method calls a main method that handles the submission logic
for a specific type of cluster manager. The top level entry point for the Kubernetes submission logic is in
`org.apache.spark.deploy.kubernetes.submit.Client#main()`.

# Driver Configuration Steps

In order to render submission parameters into the final Kubernetes driver pod specification, and do it in a scalable
manner, the submission client breaks pod construction down into a
series of configuration steps, each of which is responsible for handling some specific aspect of configuring the driver.
A top level component then iterates through all of the steps to produce a final set of Kubernetes resources that are
then deployed on the cluster.

## Interface Definitions

More formally, a configuration step must implement the following trait:

```scala
package org.apache.spark.deploy.kubernetes.submit.submitsteps

/**
 * Represents a step in preparing the Kubernetes driver.
 */
private[spark] trait DriverConfigurationStep {

  /**
   * Apply some transformation to the previous state of the driver to add a new feature to it.
   */
  def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec
}
```

A `DriverConfigurationStep` is thus a function that transforms a `KubernetesDriverSpec` into another
`KubernetesDriverSpec`, by taking the original specification and making additions to the specification in accordance to
the specific feature that step is responsible for. A `KubernetesDriverSpec` is a data structure with the following
properties:

```scala
private[spark] case class KubernetesDriverSpec(
    driverPod: Pod,
    driverContainer: Container,
    otherKubernetesResources: Seq[HasMetadata],
    driverSparkConf: SparkConf)
```

The `Pod` and `Container` classes are Java representations of Kubernetes pods and containers respectively, and the
`HasMetadata` type corresponds to an arbitrary Kubernetes API resource such as a `Secret` or a `ConfigMap`. Kubernetes
primitives are represented using an [open-source Java Kubernetes client](https://github.com/fabric8io/kubernetes-client).
The `otherKubernetesResources` field represents Kubernetes resources that are required by the Spark application. For
example, the driver may require a `ConfigMap` or `Secret` resource to be created that will be mounted into the driver
container.

## Requirements for Configuration Steps

Configuration steps must be *independent*. A given configuration step should not be opinionated about the other
configuration steps that are executed before or after it. By extension, configuration steps should be *strictly
additive*. A given configuration step should not attempt to mutate an existing field nor remove fields set in the
input driver specification.

## Composition of Configuration Steps

Finally, configuration steps are wired together by an **orchestrator**. The orchestrator effectively translates the
parameters sent to `spark-submit` into the set of steps required to configure the final `KubernetesDriverSpec`. The
top level submission client takes the final `KubernetesDriverSpec` object and builds the final requests to the
Kubernetes API server to deploy the Kubernetes resources that comprise the Spark driver. The top level submission
process can thus be expressed as follows in pseudo-code with roughly Scala syntax:

```scala
def runApplication(sparkSubmitArguments: SparkSubmitArguments) {
  val initialSpec = createEmptyDriverSpec()
  val orchestrator = new DriverConfigurationStepsOrchestrator(sparkSubmitArguments)
  val steps = orchestrator.getSubmissionSteps()
  var currentSpec = initialSpec
  // iteratively apply the configuration steps to build up the pod spec:
  for (step <- steps) {
    currentSpec = step.configureDriver(currentSpec)
  }
  // Put the container in the pod spec
  val resolvedPod = attachContainer(currentSpec.driverPod, currentSpec.driverContainer)
  kubernetes.create(resolvedPod + currentSpec.otherKubernetesResources)
}
```

## Writing a New Configuration Step

All configuration steps should be placed in the `org.apache.spark.deploy.kubernetes.submit.submitsteps` package.
Examples of other configuration steps can be found in this package as well. Ensure that the new configuration step is
returned in `org.apache.spark.deploy.kubernetes.submit.DriverConfigurationStepsOrchestrator#getAllConfigurationSteps()`.

# Dependency Management

Spark applications typically depend on binaries and various configuration files which are hosted in various locations.
Kubernetes applications typically bundle binary dependencies such as jars inside Docker images. However, Spark's API
fundamentally allows dependencies to be provided from many other locations, including the submitter's local disk.
These dependencies have to be deployed into the driver and executor containers before they run. This is challenging
because unlike Hadoop YARN which requires co-deployment with an HDFS cluster, Kubernetes clusters do not have a
large-scale persistent storage layer that would be available across every Kubernetes cluster.

## Resource Staging Server

The *resource staging server* is a lightweight daemon that serves as a file store for application dependencies. It has
two endpoints which effectively correspond to putting files into the server and getting files out of the server. When
files are put into the server, the server returns a unique identifier and a secret token in the response to the client.
This identifier and secret token must be provided when a client makes a request to retrieve the files that were uploaded
to the server.

### Resource Staging Server API Definition

The resource staging server has the following Scala API which would then be translated into HTTP endpoints via Jetty and
JAX-RS. Associated structures passed as input and output are also defined below:

```scala
private[spark] trait ResourceStagingService {

  def uploadResources(resources: InputStream, resourcesOwner: StagedResourcesOwner): SubmittedResourceIdAndSecret
  def downloadResources(resourceId: String, resourceSecret: String): StreamingOutput
}

case class StagedResourcesOwner(
    ownerNamespace: String,
    ownerLabels: Map[String, String],
    ownerType: StagedResourcesOwnerType.OwnerType)
    
// Pseudo-code to represent an enum
enum StagedResourcesOwnerType.OwnerType = { Pod }

case class SubmittedResourceIdAndSecret(resourceId: String, resourceSecret: String)
```

Clients that send resources to the server do so in a streaming manner so that both the server and the client do not
need to hold the entire resource bundle in memory. Aside from the notion of the `StagedResourcesOwner` that is provided
on uploads and not for downloads, uploading is symmetrical to downloading. The significance of the
`StagedResourcesOwner` is discussed below.

### Cleaning Up Stale Resources

The resource staging server is built to provide resources for the pods and containers in a Kubernetes cluster. These
pods are ephemeral, so at some point there will be no need for the resources that were sent for a specific application.
Clients indicate the set of resources that would be using a given resource bundle by providing a description of the
resource's "owner". The `StagedResourceOwner` is this description, defining the owner as a Kubernetes API object in
a given namespace and having a specific set of labels.

The resource staging server keeps track of the resources that were sent to it. When the resource is first uploaded, it
is marked as "unused". If the resource remains unused for a period of time, it is cleaned up. A resource is marked as
"used" when a request is made to download it. After that, the server periodically checks the API server to see if any
Kubernetes API objects exist that match the description of the owner. If no such objects exist, then resource staging
server cleans up the uploaded resource. See `org.apache.spark.deploy.rest.kubernetes.StagedResourcesCleaner` for the
code that manages the resource's lifecycle.

A resource owner can currently only be a pod, but hypothetically one could want to tie the lifetime of a resource to the
lifetime of many pods under a higher level Kubernetes object like a Deployment or a StatefulSet, all of which depend on
the uploaded resource. The resource staging server's API can be extended to tie ownership of a resource to any
Kubernetes API object type, as long as we update the `StagedResourcesOwnerType` enumeration accordingly.

### Usage in Spark

Spark-submit supports adding jars and files by passing `--jars` and `--files` to `spark-submit` respectively. The spark
configurations `spark.jars` and `spark.files` can also be set to provide this information. The submission client
determines the list of jars and files that the application needs, and it determines if any of them are files being sent
from the submitter's local machine. If any files are being sent from the local machine, the user must have specified a
URL for the resource staging server to send the files to.

Local jars and files are compacted into a tarball which are then uploaded to the resource staging server. The submission
client then knows the secret token that the driver and executors must use to download the files again. These secrets
are mounted into an [init-container](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/)
that runs before the driver and executor processes run, and the init-container
downloads the uploaded resources from the resource staging server.

### Other Considered Alternatives

The resource staging server was considered the best option among other alternative solutions to this problem.

A first implementation effectively included the resource staging server in the driver container itself. The driver
container ran a custom command that opened an HTTP endpoint and waited for the submission client to send resources to
it. The server would then run the driver application after it had received the resources from the user's local
machine. The problem with this approach is that the submission client needs to deploy the driver in such a way that the
driver itself would be reachable from outside of the cluster, but it is difficult for an automated framework which is
not aware of the cluster's configuration to expose an arbitrary pod in a generic way. The resource staging server allows
a cluster administrator to expose the resource staging server in a manner that makes sense for their cluster, such as
with an Ingress or with a NodePort service.

It is also impossible to use Kubernetes API objects like Secrets or ConfigMaps to store application binaries. These
objects require their contents to be small so that they can fit in etcd.

Finally, as mentioned before, the submission client should not be opinionated about storing dependencies in a
distributed storage system like HDFS, because not all Kubernetes clusters will have the same types of persistent storage
layers. Spark supports fetching jars directly from distributed storage layers though, so users can feel free to manually
push their dependencies to their appropriate systems and refer to them by their remote URIs in the submission request.

## Init-Containers

The driver pod and executor pods both use [init-containers](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/)
to localize resources before the driver and
executor processes launch. As mentioned before, the init-container fetches dependencies from the resource staging
server. However, even if the resource staging server is not being used, files still need to be localized from remote
locations such as HDFS clusters or HTTP file servers. The init-container will fetch these dependencies accordingly as
well.

Init-containers were preferred over fetching the dependencies in the main container primarily because this allows the
main container's runtime commands to be simplified. Using init-containers to fetch these remote dependencies allows the
main image command to simply be an invocation of Java that runs the user's main class directly. The execution of the
file localizer process can also be shared by both the driver and the executor images without needing to be copied
into both image commands. Finally, it becomes easier to debug localization failures as they will be easily spotted as
being a failure in the pod's initialization lifecycle phase.

# Future Work

* The driver's pod specification should be highly customizable, to the point where users may want to specify a template
pod spec in a YAML file: https://github.com/apache-spark-on-k8s/spark/issues/38.
* The resource staging server can be backed by a distributed file store like HDFS to improve robustness and scalability.
* Additional driver bootstrap steps need to be added to support communication with Kerberized HDFS clusters:
  * https://github.com/apache-spark-on-k8s/spark/pull/391
