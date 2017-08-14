---
layout: global
title: Kubernetes Implementation of the Spark Scheduler Backend
---

# Scheduler Backend

The general idea is to run Spark drivers and executors inside Kubernetes [Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod/). 
Pods are a co-located and co-scheduled group of one or more containers run in a shared context. The main component is KubernetesClusterSchedulerBackend, 
an implementation of CoarseGrainedSchedulerBackend, which manages allocating and destroying executors via the Kubernetes API. 
There are auxiliary and optional components: `ResourceStagingServer` and `KubernetesExternalShuffleService`, which serve specific purposes described further below.

The scheduler backend is invoked in the driver associated with a particular job. The driver may run outside the cluster (client mode) or within (cluster mode). 
The scheduler backend manages [pods](http://kubernetes.io/docs/user-guide/pods/) for each executor. 
The executor code is running within a Kubernetes pod, but remains unmodified and unaware of the orchestration layer. 
When a job is running, the scheduler backend configures and creates executor pods with the following properties:

- The pod's container runs a pre-built Docker image containing a Spark distribution (with Kubernetes integration) and 
invokes the Java runtime with the CoarseGrainedExecutorBackend main class.
- The scheduler backend specifies environment variables on the executor pod to configure its runtime, p
articularly for its JVM options, number of cores, heap size, and the driver's hostname.
- The executor container has [resource limits and requests](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container) 
that are set in accordance to the resource limits specified in the Spark configuration (executor.cores and executor.memory in the application's SparkConf)
- The executor pods may also be launched into a particular [Kubernetes namespace](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/%5C), 
or target a particular subset of nodes in the Kubernetes cluster, based on the Spark configuration supplied.

## Requesting Executors

Spark requests for new executors through the `doRequestTotalExecutors(numExecutors: Int)` method. 
The scheduler backend keeps track of the request made by Spark core for the number of executors.

A separate kubernetes-pod-allocator thread handles the creation of new executor pods with appropriate throttling and monitoring. 
This indirection is required because the Kubernetes API Server accepts requests for new executor pods optimistically, with the 
anticipation of being able to eventually run them. However, it is undesirable to have a very large number of pods that cannot be 
scheduled and stay pending within the cluster. Hence, the kubernetes-pod-allocator uses the Kubernetes API to make a decision to 
submit new requests for executors based on whether previous pod creation requests have completed. This gives us control over how 
fast a job scales up (which can be configured), and helps prevent Spark jobs from DOS-ing the Kubernetes API server with pod creation requests.

## Destroying Executors

Spark requests deletion of executors through the `doKillExecutors(executorIds: List[String])`
method.

The inverse behavior is required in the implementation of doKillExecutors(). When the executor
allocation manager desires to remove executors from the application, the scheduler should find the
pods that are running the appropriate executors, and tell the API server to stop these pods.
It's worth noting that this code does not have to decide on the executors that should be
removed. When `doKillExecutors()` is called, the executors that are to be removed have already been
selected by the CoarseGrainedSchedulerBackend and ExecutorAllocationManager.
