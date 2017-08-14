---
layout: global
title: Kubernetes Implementation of the External Shuffle Service
---

# External Shuffle Service

The `KubernetesExternalShuffleService` was added to allow Spark to use Dynamic Allocation Mode when
running in Kubernetes. The shuffle service is responsible for persisting shuffle files beyond the
lifetime of the executors, allowing the number of executors to scale up and down without losing
computation. 

The implementation of choice is as a DaemonSet that runs a shuffle-service pod on each node.
Shuffle-service pods and executors pods that land on the same node share disk using hostpath
volumes. Spark requires that each executor must know the IP address of the shuffle-service pod that
shares disk with it. 

The user specifies the shuffle service pods they want executors of a particular SparkJob to use
through two new properties:

* spark.kubernetes.shuffle.service.labels
* spark.kubernetes.shuffle.namespace

KubernetesClusterSchedulerBackend is aware of shuffle service pods and the node corresponding to
them in a particular namespace. It uses this data to configure the executor pods to connect with the
shuffle services that are co-located with them on the same node. 

There is additional logic in the `KubernetesExternalShuffleService` to watch the Kubernetes API,
detect failures, and proactively cleanup files in those error cases. 
