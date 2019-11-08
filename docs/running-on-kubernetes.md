---
layout: global
title: Running Spark on Kubernetes
---
* This will become a table of contents (this text will be scraped).
{:toc}

Spark can run on clusters managed by [Kubernetes](https://kubernetes.io). This feature makes use of native
Kubernetes scheduler that has been added to Spark.

**The Kubernetes scheduler is currently experimental.
In future versions, there may be behavioral changes around configuration,
container images and entrypoints.**

# Security

Security in Spark is OFF by default. This could mean you are vulnerable to attack by default.
Please see [Spark Security](security.html) and the specific advice below before running Spark.

## User Identity

Images built from the project provided Dockerfiles do not contain any [`USER`](https://docs.docker.com/engine/reference/builder/#user) directives.  This means that the resulting images will be running the Spark processes as `root` inside the container.  On unsecured clusters this may provide an attack vector for privilege escalation and container breakout.  Therefore security conscious deployments should consider providing custom images with `USER` directives specifying an unprivileged UID and GID.

Alternatively the [Pod Template](#pod-template) feature can be used to add a [Security Context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#volumes-and-file-systems) with a `runAsUser` to the pods that Spark submits.  Please bear in mind that this requires cooperation from your users and as such may not be a suitable solution for shared environments.  Cluster administrators should use [Pod Security Policies](https://kubernetes.io/docs/concepts/policy/pod-security-policy/#users-and-groups) if they wish to limit the users that pods may run as.

## Volume Mounts

As described later in this document under [Using Kubernetes Volumes](#using-kubernetes-volumes) Spark on K8S provides configuration options that allow for mounting certain volume types into the driver and executor pods.  In particular it allows for [`hostPath`](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath) volumes which as described in the Kubernetes documentation have known security vulnerabilities.

Cluster administrators should use [Pod Security Policies](https://kubernetes.io/docs/concepts/policy/pod-security-policy/) to limit the ability to mount `hostPath` volumes appropriately for their environments.

# Prerequisites

* A runnable distribution of Spark 2.3 or above.
* A running Kubernetes cluster at version >= 1.6 with access configured to it using
[kubectl](https://kubernetes.io/docs/user-guide/prereqs/).  If you do not already have a working Kubernetes cluster,
you may set up a test cluster on your local machine using
[minikube](https://kubernetes.io/docs/getting-started-guides/minikube/).
  * We recommend using the latest release of minikube with the DNS addon enabled.
  * Be aware that the default minikube configuration is not enough for running Spark applications.
  We recommend 3 CPUs and 4g of memory to be able to start a simple Spark application with a single
  executor.
* You must have appropriate permissions to list, create, edit and delete
[pods](https://kubernetes.io/docs/user-guide/pods/) in your cluster. You can verify that you can list these resources
by running `kubectl auth can-i <list|create|edit|delete> pods`.
  * The service account credentials used by the driver pods must be allowed to create pods, services and configmaps.
* You must have [Kubernetes DNS](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/) configured in your cluster.

# How it works

<p style="text-align: center;">
  <img src="img/k8s-cluster-mode.png" title="Spark cluster components" alt="Spark cluster components" />
</p>

<code>spark-submit</code> can be directly used to submit a Spark application to a Kubernetes cluster.
The submission mechanism works as follows:

* Spark creates a Spark driver running within a [Kubernetes pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/).
* The driver creates executors which are also running within Kubernetes pods and connects to them, and executes application code.
* When the application completes, the executor pods terminate and are cleaned up, but the driver pod persists
logs and remains in "completed" state in the Kubernetes API until it's eventually garbage collected or manually cleaned up.

Note that in the completed state, the driver pod does *not* use any computational or memory resources.

The driver and executor pod scheduling is handled by Kubernetes. It is possible to schedule the
driver and executor pods on a subset of available nodes through a [node selector](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#nodeselector)
using the configuration property for it. It will be possible to use more advanced
scheduling hints like [node/pod affinities](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity) in a future release.

# Submitting Applications to Kubernetes

## Docker Images

Kubernetes requires users to supply images that can be deployed into containers within pods. The images are built to
be run in a container runtime environment that Kubernetes supports. Docker is a container runtime environment that is
frequently used with Kubernetes. Spark (starting with version 2.3) ships with a Dockerfile that can be used for this
purpose, or customized to match an individual application's needs. It can be found in the `kubernetes/dockerfiles/`
directory.

Spark also ships with a `bin/docker-image-tool.sh` script that can be used to build and publish the Docker images to
use with the Kubernetes backend.

Example usage is:

```bash
$ ./bin/docker-image-tool.sh -r <repo> -t my-tag build
$ ./bin/docker-image-tool.sh -r <repo> -t my-tag push
```

## Cluster Mode

To launch Spark Pi in cluster mode,

```bash
$ bin/spark-submit \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=<spark-image> \
    local:///path/to/examples.jar
```

The Spark master, specified either via passing the `--master` command line argument to `spark-submit` or by setting
`spark.master` in the application's configuration, must be a URL with the format `k8s://<api_server_host>:<k8s-apiserver-port>`. The port must always be specified, even if it's the HTTPS port 443. Prefixing the
master string with `k8s://` will cause the Spark application to launch on the Kubernetes cluster, with the API server
being contacted at `api_server_url`. If no HTTP protocol is specified in the URL, it defaults to `https`. For example,
setting the master to `k8s://example.com:443` is equivalent to setting it to `k8s://https://example.com:443`, but to
connect without TLS on a different port, the master would be set to `k8s://http://example.com:8080`.

In Kubernetes mode, the Spark application name that is specified by `spark.app.name` or the `--name` argument to
`spark-submit` is used by default to name the Kubernetes resources created like drivers and executors. So, application names
must consist of lower case alphanumeric characters, `-`, and `.`  and must start and end with an alphanumeric character.

If you have a Kubernetes cluster setup, one way to discover the apiserver URL is by executing `kubectl cluster-info`.

```bash
$ kubectl cluster-info
Kubernetes master is running at http://127.0.0.1:6443
```

In the above example, the specific Kubernetes cluster can be used with <code>spark-submit</code> by specifying
`--master k8s://http://127.0.0.1:6443` as an argument to spark-submit. Additionally, it is also possible to use the
authenticating proxy, `kubectl proxy` to communicate to the Kubernetes API.

The local proxy can be started by:

```bash
$ kubectl proxy
```

If the local proxy is running at localhost:8001, `--master k8s://http://127.0.0.1:8001` can be used as the argument to
spark-submit. Finally, notice that in the above example we specify a jar with a specific URI with a scheme of `local://`.
This URI is the location of the example jar that is already in the Docker image.

## Client Mode

Starting with Spark 2.4.0, it is possible to run Spark applications on Kubernetes in client mode. When your application
runs in client mode, the driver can run inside a pod or on a physical host. When running an application in client mode,
it is recommended to account for the following factors:

### Client Mode Networking

Spark executors must be able to connect to the Spark driver over a hostname and a port that is routable from the Spark
executors. The specific network configuration that will be required for Spark to work in client mode will vary per
setup. If you run your driver inside a Kubernetes pod, you can use a
[headless service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) to allow your
driver pod to be routable from the executors by a stable hostname. When deploying your headless service, ensure that
the service's label selector will only match the driver pod and no other pods; it is recommended to assign your driver
pod a sufficiently unique label and to use that label in the label selector of the headless service. Specify the driver's
hostname via `spark.driver.host` and your spark driver's port to `spark.driver.port`.

### Client Mode Executor Pod Garbage Collection

If you run your Spark driver in a pod, it is highly recommended to set `spark.kubernetes.driver.pod.name` to the name of that pod.
When this property is set, the Spark scheduler will deploy the executor pods with an
[OwnerReference](https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/), which in turn will
ensure that once the driver pod is deleted from the cluster, all of the application's executor pods will also be deleted.
The driver will look for a pod with the given name in the namespace specified by `spark.kubernetes.namespace`, and
an OwnerReference pointing to that pod will be added to each executor pod's OwnerReferences list. Be careful to avoid
setting the OwnerReference to a pod that is not actually that driver pod, or else the executors may be terminated
prematurely when the wrong pod is deleted.

If your application is not running inside a pod, or if `spark.kubernetes.driver.pod.name` is not set when your application is
actually running in a pod, keep in mind that the executor pods may not be properly deleted from the cluster when the
application exits. The Spark scheduler attempts to delete these pods, but if the network request to the API server fails
for any reason, these pods will remain in the cluster. The executor processes should exit when they cannot reach the
driver, so the executor pods should not consume compute resources (cpu and memory) in the cluster after your application
exits.

### Authentication Parameters

Use the exact prefix `spark.kubernetes.authenticate` for Kubernetes authentication parameters in client mode.

## Dependency Management

If your application's dependencies are all hosted in remote locations like HDFS or HTTP servers, they may be referred to
by their appropriate remote URIs. Also, application dependencies can be pre-mounted into custom-built Docker images.
Those dependencies can be added to the classpath by referencing them with `local://` URIs and/or setting the
`SPARK_EXTRA_CLASSPATH` environment variable in your Dockerfiles. The `local://` scheme is also required when referring to
dependencies in custom-built Docker images in `spark-submit`. Note that using application dependencies from the submission
client's local file system is currently not yet supported.

## Secret Management
Kubernetes [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/) can be used to provide credentials for a
Spark application to access secured services. To mount a user-specified secret into the driver container, users can use
the configuration property of the form `spark.kubernetes.driver.secrets.[SecretName]=<mount path>`. Similarly, the
configuration property of the form `spark.kubernetes.executor.secrets.[SecretName]=<mount path>` can be used to mount a
user-specified secret into the executor containers. Note that it is assumed that the secret to be mounted is in the same
namespace as that of the driver and executor pods. For example, to mount a secret named `spark-secret` onto the path
`/etc/secrets` in both the driver and executor containers, add the following options to the `spark-submit` command:

```
--conf spark.kubernetes.driver.secrets.spark-secret=/etc/secrets
--conf spark.kubernetes.executor.secrets.spark-secret=/etc/secrets
```

To use a secret through an environment variable use the following options to the `spark-submit` command:
```
--conf spark.kubernetes.driver.secretKeyRef.ENV_NAME=name:key
--conf spark.kubernetes.executor.secretKeyRef.ENV_NAME=name:key
```

## Using Kubernetes Volumes

Starting with Spark 2.4.0, users can mount the following types of Kubernetes [volumes](https://kubernetes.io/docs/concepts/storage/volumes/) into the driver and executor pods:
* [hostPath](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath): mounts a file or directory from the host node’s filesystem into a pod.
* [emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir): an initially empty volume created when a pod is assigned to a node.
* [persistentVolumeClaim](https://kubernetes.io/docs/concepts/storage/volumes/#persistentvolumeclaim): used to mount a `PersistentVolume` into a pod.

**NB:** Please see the [Security](#security) section of this document for security issues related to volume mounts.

To mount a volume of any of the types above into the driver pod, use the following configuration property:

```
--conf spark.kubernetes.driver.volumes.[VolumeType].[VolumeName].mount.path=<mount path>
--conf spark.kubernetes.driver.volumes.[VolumeType].[VolumeName].mount.readOnly=<true|false>
``` 

Specifically, `VolumeType` can be one of the following values: `hostPath`, `emptyDir`, and `persistentVolumeClaim`. `VolumeName` is the name you want to use for the volume under the `volumes` field in the pod specification.

Each supported type of volumes may have some specific configuration options, which can be specified using configuration properties of the following form:

```
spark.kubernetes.driver.volumes.[VolumeType].[VolumeName].options.[OptionName]=<value>
``` 

For example, the claim name of a `persistentVolumeClaim` with volume name `checkpointpvc` can be specified using the following property:

```
spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpointpvc.options.claimName=check-point-pvc-claim
```

The configuration properties for mounting volumes into the executor pods use prefix `spark.kubernetes.executor.` instead of `spark.kubernetes.driver.`. For a complete list of available options for each supported type of volumes, please refer to the [Spark Properties](#spark-properties) section below. 

## Introspection and Debugging

These are the different ways in which you can investigate a running/completed Spark application, monitor progress, and
take actions.

### Accessing Logs

Logs can be accessed using the Kubernetes API and the `kubectl` CLI. When a Spark application is running, it's possible
to stream logs from the application using:

```bash
$ kubectl -n=<namespace> logs -f <driver-pod-name>
```

The same logs can also be accessed through the
[Kubernetes dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) if installed on
the cluster.

### Accessing Driver UI

The UI associated with any application can be accessed locally using
[`kubectl port-forward`](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/#forward-a-local-port-to-a-port-on-the-pod).

```bash
$ kubectl port-forward <driver-pod-name> 4040:4040
```

Then, the Spark driver UI can be accessed on `http://localhost:4040`.

### Debugging

There may be several kinds of failures. If the Kubernetes API server rejects the request made from spark-submit, or the
connection is refused for a different reason, the submission logic should indicate the error encountered. However, if there
are errors during the running of the application, often, the best way to investigate may be through the Kubernetes CLI.

To get some basic information about the scheduling decisions made around the driver pod, you can run:

```bash
$ kubectl describe pod <spark-driver-pod>
```

If the pod has encountered a runtime error, the status can be probed further using:

```bash
$ kubectl logs <spark-driver-pod>
```

Status and logs of failed executor pods can be checked in similar ways. Finally, deleting the driver pod will clean up the entire spark
application, including all executors, associated service, etc. The driver pod can be thought of as the Kubernetes representation of
the Spark application.

## Kubernetes Features

### Namespaces

Kubernetes has the concept of [namespaces](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/).
Namespaces are ways to divide cluster resources between multiple users (via resource quota). Spark on Kubernetes can
use namespaces to launch Spark applications. This can be made use of through the `spark.kubernetes.namespace` configuration.

Kubernetes allows using [ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/) to set limits on
resources, number of objects, etc on individual namespaces. Namespaces and ResourceQuota can be used in combination by
administrator to control sharing and resource allocation in a Kubernetes cluster running Spark applications.

### RBAC

In Kubernetes clusters with [RBAC](https://kubernetes.io/docs/admin/authorization/rbac/) enabled, users can configure
Kubernetes RBAC roles and service accounts used by the various Spark on Kubernetes components to access the Kubernetes
API server.

The Spark driver pod uses a Kubernetes service account to access the Kubernetes API server to create and watch executor
pods. The service account used by the driver pod must have the appropriate permission for the driver to be able to do
its work. Specifically, at minimum, the service account must be granted a
[`Role` or `ClusterRole`](https://kubernetes.io/docs/admin/authorization/rbac/#role-and-clusterrole) that allows driver
pods to create pods and services. By default, the driver pod is automatically assigned the `default` service account in
the namespace specified by `spark.kubernetes.namespace`, if no service account is specified when the pod gets created.

Depending on the version and setup of Kubernetes deployed, this `default` service account may or may not have the role
that allows driver pods to create pods and services under the default Kubernetes
[RBAC](https://kubernetes.io/docs/admin/authorization/rbac/) policies. Sometimes users may need to specify a custom
service account that has the right role granted. Spark on Kubernetes supports specifying a custom service account to
be used by the driver pod through the configuration property
`spark.kubernetes.authenticate.driver.serviceAccountName=<service account name>`. For example, to make the driver pod
use the `spark` service account, a user simply adds the following option to the `spark-submit` command:

```
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
```

To create a custom service account, a user can use the `kubectl create serviceaccount` command. For example, the
following command creates a service account named `spark`:

```bash
$ kubectl create serviceaccount spark
```

To grant a service account a `Role` or `ClusterRole`, a `RoleBinding` or `ClusterRoleBinding` is needed. To create
a `RoleBinding` or `ClusterRoleBinding`, a user can use the `kubectl create rolebinding` (or `clusterrolebinding`
for `ClusterRoleBinding`) command. For example, the following command creates an `edit` `ClusterRole` in the `default`
namespace and grants it to the `spark` service account created above:

```bash
$ kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```

Note that a `Role` can only be used to grant access to resources (like pods) within a single namespace, whereas a
`ClusterRole` can be used to grant access to cluster-scoped resources (like nodes) as well as namespaced resources
(like pods) across all namespaces. For Spark on Kubernetes, since the driver always creates executor pods in the
same namespace, a `Role` is sufficient, although users may use a `ClusterRole` instead. For more information on
RBAC authorization and how to configure Kubernetes service accounts for pods, please refer to
[Using RBAC Authorization](https://kubernetes.io/docs/admin/authorization/rbac/) and
[Configure Service Accounts for Pods](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/).

## Future Work

There are several Spark on Kubernetes features that are currently being worked on or planned to be worked on. Those features are expected to eventually make it into future versions of the spark-kubernetes integration.

Some of these include:

* Dynamic Resource Allocation and External Shuffle Service
* Local File Dependency Management
* Spark Application Management
* Job Queues and Resource Management

# Configuration

See the [configuration page](configuration.html) for information on Spark configurations.  The following configurations are
specific to Spark on Kubernetes.

#### Spark Properties

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.kubernetes.namespace</code></td>
  <td><code>default</code></td>
  <td>
    The namespace that will be used for running the driver and executor pods.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.container.image</code></td>
  <td><code>(none)</code></td>
  <td>
    Container image to use for the Spark application.
    This is usually of the form <code>example.com/repo/spark:v1.0.0</code>.
    This configuration is required and must be provided by the user, unless explicit
    images are provided for each different container type.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.container.image</code></td>
  <td><code>(value of spark.kubernetes.container.image)</code></td>
  <td>
    Custom container image to use for the driver.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.container.image</code></td>
  <td><code>(value of spark.kubernetes.container.image)</code></td>
  <td>
    Custom container image to use for executors.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.container.image.pullPolicy</code></td>
  <td><code>IfNotPresent</code></td>
  <td>
    Container image pull policy used when pulling images within Kubernetes.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.container.image.pullSecrets</code></td>
  <td><code></code></td>
  <td>
    Comma separated list of Kubernetes secrets used to pull images from private image registries.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.allocation.batch.size</code></td>
  <td><code>5</code></td>
  <td>
    Number of pods to launch at once in each round of executor pod allocation.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.allocation.batch.delay</code></td>
  <td><code>1s</code></td>
  <td>
    Time to wait between each round of executor pod allocation. Specifying values less than 1 second may lead to
    excessive CPU usage on the spark driver.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.caCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the CA cert file for connecting to the Kubernetes API server over TLS when starting the driver. This file
    must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not provide
    a scheme). In client mode, use <code>spark.kubernetes.authenticate.caCertFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server when starting the driver. This file
    must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not provide
    a scheme). In client mode, use <code>spark.kubernetes.authenticate.clientKeyFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server when starting the driver. This
    file must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not
    provide a scheme). In client mode, use <code>spark.kubernetes.authenticate.clientCertFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.oauthToken</code></td>
  <td>(none)</td>
  <td>
    OAuth token to use when authenticating against the Kubernetes API server when starting the driver. Note
    that unlike the other authentication options, this is expected to be the exact string value of the token to use for
    the authentication. In client mode, use <code>spark.kubernetes.authenticate.oauthToken</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    Path to the OAuth token file containing the token to use when authenticating against the Kubernetes API server when starting the driver.
    This file must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not
    provide a scheme). In client mode, use <code>spark.kubernetes.authenticate.oauthTokenFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.caCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the CA cert file for connecting to the Kubernetes API server over TLS from the driver pod when requesting
    executors. This file must be located on the submitting machine's disk, and will be uploaded to the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme). In client mode, use
    <code>spark.kubernetes.authenticate.caCertFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server from the driver pod when requesting
    executors. This file must be located on the submitting machine's disk, and will be uploaded to the driver pod as
    a Kubernetes secret. Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
    In client mode, use <code>spark.kubernetes.authenticate.clientKeyFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server from the driver pod when
    requesting executors. This file must be located on the submitting machine's disk, and will be uploaded to the
    driver pod as a Kubernetes secret. Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
    In client mode, use <code>spark.kubernetes.authenticate.clientCertFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.oauthToken</code></td>
  <td>(none)</td>
  <td>
    OAuth token to use when authenticating against the Kubernetes API server from the driver pod when
    requesting executors. Note that unlike the other authentication options, this must be the exact string value of
    the token to use for the authentication. This token value is uploaded to the driver pod as a Kubernetes secret.
    In client mode, use <code>spark.kubernetes.authenticate.oauthToken</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    Path to the OAuth token file containing the token to use when authenticating against the Kubernetes API server from the driver pod when
    requesting executors. Note that unlike the other authentication options, this file must contain the exact string value of
    the token to use for the authentication. This token value is uploaded to the driver pod as a secret. In client mode, use
    <code>spark.kubernetes.authenticate.oauthTokenFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.mounted.caCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the CA cert file for connecting to the Kubernetes API server over TLS from the driver pod when requesting
    executors. This path must be accessible from the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme). In client mode, use
    <code>spark.kubernetes.authenticate.caCertFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.mounted.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server from the driver pod when requesting
    executors. This path must be accessible from the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme). In client mode, use
    <code>spark.kubernetes.authenticate.clientKeyFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.mounted.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server from the driver pod when
    requesting executors. This path must be accessible from the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme). In client mode, use
    <code>spark.kubernetes.authenticate.clientCertFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.mounted.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    Path to the file containing the OAuth token to use when authenticating against the Kubernetes API server from the driver pod when
    requesting executors. This path must be accessible from the driver pod.
    Note that unlike the other authentication options, this file must contain the exact string value of the token to use
    for the authentication. In client mode, use <code>spark.kubernetes.authenticate.oauthTokenFile</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.serviceAccountName</code></td>
  <td><code>default</code></td>
  <td>
    Service account that is used when running the driver pod. The driver pod uses this service account when requesting
    executor pods from the API server. Note that this cannot be specified alongside a CA cert file, client key file,
    client cert file, and/or OAuth token. In client mode, use <code>spark.kubernetes.authenticate.serviceAccountName</code> instead.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.caCertFile</code></td>
  <td>(none)</td>
  <td>
    In client mode, path to the CA cert file for connecting to the Kubernetes API server over TLS when
    requesting executors. Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    In client mode, path to the client key file for authenticating against the Kubernetes API server
    when requesting executors. Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    In client mode, path to the client cert file for authenticating against the Kubernetes API server
    when requesting executors. Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.oauthToken</code></td>
  <td>(none)</td>
  <td>
    In client mode, the OAuth token to use when authenticating against the Kubernetes API server when
    requesting executors. Note that unlike the other authentication options, this must be the exact string value of
    the token to use for the authentication.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    In client mode, path to the file containing the OAuth token to use when authenticating against the Kubernetes API
    server when requesting executors.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.label.[LabelName]</code></td>
  <td>(none)</td>
  <td>
    Add the label specified by <code>LabelName</code> to the driver pod.
    For example, <code>spark.kubernetes.driver.label.something=true</code>.
    Note that Spark also adds its own labels to the driver pod
    for bookkeeping purposes.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.annotation.[AnnotationName]</code></td>
  <td>(none)</td>
  <td>
    Add the annotation specified by <code>AnnotationName</code> to the driver pod.
    For example, <code>spark.kubernetes.driver.annotation.something=true</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.label.[LabelName]</code></td>
  <td>(none)</td>
  <td>
    Add the label specified by <code>LabelName</code> to the executor pods.
    For example, <code>spark.kubernetes.executor.label.something=true</code>.
    Note that Spark also adds its own labels to the driver pod
    for bookkeeping purposes.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.annotation.[AnnotationName]</code></td>
  <td>(none)</td>
  <td>
    Add the annotation specified by <code>AnnotationName</code> to the executor pods.
    For example, <code>spark.kubernetes.executor.annotation.something=true</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.pod.name</code></td>
  <td>(none)</td>
  <td>
    Name of the driver pod. In cluster mode, if this is not set, the driver pod name is set to "spark.app.name"
    suffixed by the current timestamp to avoid name conflicts. In client mode, if your application is running
    inside a pod, it is highly recommended to set this to the name of the pod your driver is running in. Setting this
    value in client mode allows the driver to become the owner of its executor pods, which in turn allows the executor
    pods to be garbage collected by the cluster.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.lostCheck.maxAttempts</code></td>
  <td><code>10</code></td>
  <td>
    Number of times that the driver will try to ascertain the loss reason for a specific executor.
    The loss reason is used to ascertain whether the executor failure is due to a framework or an application error
    which in turn decides whether the executor is removed and replaced, or placed into a failed state for debugging.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submission.waitAppCompletion</code></td>
  <td><code>true</code></td>
  <td>
    In cluster mode, whether to wait for the application to finish before exiting the launcher process.  When changed to
    false, the launcher has a "fire-and-forget" behavior when launching the Spark job.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.report.interval</code></td>
  <td><code>1s</code></td>
  <td>
    Interval between reports of the current Spark job status in cluster mode.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.limit.cores</code></td>
  <td>(none)</td>
  <td>
    Specify a hard cpu <a href="https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container">limit</a> for the driver pod.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.request.cores</code></td>
  <td>(none)</td>
  <td>
    Specify the cpu request for each executor pod. Values conform to the Kubernetes <a href="https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-cpu">convention</a>.
    Example values include 0.1, 500m, 1.5, 5, etc., with the definition of cpu units documented in <a href="https://kubernetes.io/docs/tasks/configure-pod-container/assign-cpu-resource/#cpu-units">CPU units</a>.
    This is distinct from <code>spark.executor.cores</code>: it is only used and takes precedence over <code>spark.executor.cores</code> for specifying the executor pod cpu request if set. Task 
    parallelism, e.g., number of tasks an executor can run concurrently is not affected by this.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.limit.cores</code></td>
  <td>(none)</td>
  <td>
    Specify a hard cpu <a href="https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container">limit</a> for each executor pod launched for the Spark Application.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.node.selector.[labelKey]</code></td>
  <td>(none)</td>
  <td>
    Adds to the node selector of the driver pod and executor pods, with key <code>labelKey</code> and the value as the
    configuration's value. For example, setting <code>spark.kubernetes.node.selector.identifier</code> to <code>myIdentifier</code>
    will result in the driver pod and executors having a node selector with key <code>identifier</code> and value
     <code>myIdentifier</code>. Multiple node selector keys can be added by setting multiple configurations with this prefix.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driverEnv.[EnvironmentVariableName]</code></td>
  <td>(none)</td>
  <td>
    Add the environment variable specified by <code>EnvironmentVariableName</code> to
    the Driver process. The user can specify multiple of these to set multiple environment variables.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.secrets.[SecretName]</code></td>
  <td>(none)</td>
  <td>
   Add the <a href="https://kubernetes.io/docs/concepts/configuration/secret/">Kubernetes Secret</a> named <code>SecretName</code> to the driver pod on the path specified in the value. For example,
   <code>spark.kubernetes.driver.secrets.spark-secret=/etc/secrets</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.secrets.[SecretName]</code></td>
  <td>(none)</td>
  <td>
   Add the <a href="https://kubernetes.io/docs/concepts/configuration/secret/">Kubernetes Secret</a> named <code>SecretName</code> to the executor pod on the path specified in the value. For example,
   <code>spark.kubernetes.executor.secrets.spark-secret=/etc/secrets</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.secretKeyRef.[EnvName]</code></td>
  <td>(none)</td>
  <td>
   Add as an environment variable to the driver container with name EnvName (case sensitive), the value referenced by key <code> key </code> in the data of the referenced <a href="https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables">Kubernetes Secret</a>. For example,
   <code>spark.kubernetes.driver.secretKeyRef.ENV_VAR=spark-secret:key</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.secretKeyRef.[EnvName]</code></td>
  <td>(none)</td>
  <td>
   Add as an environment variable to the executor container with name EnvName (case sensitive), the value referenced by key <code> key </code> in the data of the referenced <a href="https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables">Kubernetes Secret</a>. For example,
   <code>spark.kubernetes.executor.secrets.ENV_VAR=spark-secret:key</code>.
  </td>
</tr>   
<tr>
  <td><code>spark.kubernetes.driver.volumes.[VolumeType].[VolumeName].mount.path</code></td>
  <td>(none)</td>
  <td>
   Add the <a href="https://kubernetes.io/docs/concepts/storage/volumes/">Kubernetes Volume</a> named <code>VolumeName</code> of the <code>VolumeType</code> type to the driver pod on the path specified in the value. For example,
   <code>spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpointpvc.mount.path=/checkpoint</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.volumes.[VolumeType].[VolumeName].mount.readOnly</code></td>
  <td>(none)</td>
  <td>
   Specify if the mounted volume is read only or not. For example,
   <code>spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpointpvc.mount.readOnly=false</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.volumes.[VolumeType].[VolumeName].options.[OptionName]</code></td>
  <td>(none)</td>
  <td>
   Configure <a href="https://kubernetes.io/docs/concepts/storage/volumes/">Kubernetes Volume</a> options passed to the Kubernetes with <code>OptionName</code> as key having specified value, must conform with Kubernetes option format. For example,
   <code>spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpointpvc.options.claimName=spark-pvc-claim</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.volumes.[VolumeType].[VolumeName].mount.path</code></td>
  <td>(none)</td>
  <td>
   Add the <a href="https://kubernetes.io/docs/concepts/storage/volumes/">Kubernetes Volume</a> named <code>VolumeName</code> of the <code>VolumeType</code> type to the executor pod on the path specified in the value. For example,
   <code>spark.kubernetes.executor.volumes.persistentVolumeClaim.checkpointpvc.mount.path=/checkpoint</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.volumes.[VolumeType].[VolumeName].mount.readOnly</code></td>
  <td>false</td>
  <td>
   Specify if the mounted volume is read only or not. For example,
   <code>spark.kubernetes.executor.volumes.persistentVolumeClaim.checkpointpvc.mount.readOnly=false</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.volumes.[VolumeType].[VolumeName].options.[OptionName]</code></td>
  <td>(none)</td>
  <td>
   Configure <a href="https://kubernetes.io/docs/concepts/storage/volumes/">Kubernetes Volume</a> options passed to the Kubernetes with <code>OptionName</code> as key having specified value. For example,
   <code>spark.kubernetes.executor.volumes.persistentVolumeClaim.checkpointpvc.options.claimName=spark-pvc-claim</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.memoryOverheadFactor</code></td>
  <td><code>0.1</code></td>
  <td>
    This sets the Memory Overhead Factor that will allocate memory to non-JVM memory, which includes off-heap memory allocations, non-JVM tasks, and various systems processes. For JVM-based jobs this value will default to 0.10 and 0.40 for non-JVM jobs.
    This is done as non-JVM tasks need more non-JVM heap space and such tasks commonly fail with "Memory Overhead Exceeded" errors. This prempts this error with a higher default. 
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.pyspark.pythonVersion</code></td>
  <td><code>"2"</code></td>
  <td>
   This sets the major Python version of the docker image used to run the driver and executor containers. Can either be 2 or 3. 
  </td>
</tr>
</table>
