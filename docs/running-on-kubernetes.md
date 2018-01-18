---
layout: global
title: Running Spark on Kubernetes
---
* This will become a table of contents (this text will be scraped).
{:toc}

Spark can run on clusters managed by [Kubernetes](https://kubernetes.io). This feature makes use of native
Kubernetes scheduler that has been added to Spark.

# Prerequisites

* A runnable distribution of Spark 2.3 or above.
* A running Kubernetes cluster at version >= 1.6 with access configured to it using
[kubectl](https://kubernetes.io/docs/user-guide/prereqs/).  If you do not already have a working Kubernetes cluster,
you may setup a test cluster on your local machine using
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

The driver and executor pod scheduling is handled by Kubernetes. It will be possible to affect Kubernetes scheduling
decisions for driver and executor pods using advanced primitives like
[node selectors](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#nodeselector)
and [node/pod affinities](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity)
in a future release.

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

    ./bin/docker-image-tool.sh -r <repo> -t my-tag build
    ./bin/docker-image-tool.sh -r <repo> -t my-tag push

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
`spark.master` in the application's configuration, must be a URL with the format `k8s://<api_server_url>`. Prefixing the
master string with `k8s://` will cause the Spark application to launch on the Kubernetes cluster, with the API server
being contacted at `api_server_url`. If no HTTP protocol is specified in the URL, it defaults to `https`. For example,
setting the master to `k8s://example.com:443` is equivalent to setting it to `k8s://https://example.com:443`, but to
connect without TLS on a different port, the master would be set to `k8s://http://example.com:8080`.

In Kubernetes mode, the Spark application name that is specified by `spark.app.name` or the `--name` argument to
`spark-submit` is used by default to name the Kubernetes resources created like drivers and executors. So, application names
must consist of lower case alphanumeric characters, `-`, and `.`  and must start and end with an alphanumeric character.

If you have a Kubernetes cluster setup, one way to discover the apiserver URL is by executing `kubectl cluster-info`.

```bash
kubectl cluster-info
Kubernetes master is running at http://127.0.0.1:6443
```

In the above example, the specific Kubernetes cluster can be used with <code>spark-submit</code> by specifying
`--master k8s://http://127.0.0.1:6443` as an argument to spark-submit. Additionally, it is also possible to use the
authenticating proxy, `kubectl proxy` to communicate to the Kubernetes API.

The local proxy can be started by:

```bash
kubectl proxy
```

If the local proxy is running at localhost:8001, `--master k8s://http://127.0.0.1:8001` can be used as the argument to
spark-submit. Finally, notice that in the above example we specify a jar with a specific URI with a scheme of `local://`.
This URI is the location of the example jar that is already in the Docker image.

## Dependency Management

If your application's dependencies are all hosted in remote locations like HDFS or HTTP servers, they may be referred to
by their appropriate remote URIs. Also, application dependencies can be pre-mounted into custom-built Docker images.
Those dependencies can be added to the classpath by referencing them with `local://` URIs and/or setting the
`SPARK_EXTRA_CLASSPATH` environment variable in your Dockerfiles. The `local://` scheme is also required when referring to
dependencies in custom-built Docker images in `spark-submit`. Note that using application dependencies local to the submission
client is currently not yet supported.


### Using Remote Dependencies
When there are application dependencies hosted in remote locations like HDFS or HTTP servers, the driver and executor pods
need a Kubernetes [init-container](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) for downloading
the dependencies so the driver and executor containers can use them locally.

The init-container handles remote dependencies specified in `spark.jars` (or the `--jars` option of `spark-submit`) and
`spark.files` (or the `--files` option of `spark-submit`). It also handles remotely hosted main application resources, e.g.,
the main application jar. The following shows an example of using remote dependencies with the `spark-submit` command:

```bash
$ bin/spark-submit \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --jars https://path/to/dependency1.jar,https://path/to/dependency2.jar
    --files hdfs://host:port/path/to/file1,hdfs://host:port/path/to/file2
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=<spark-image> \
    https://path/to/examples.jar
```

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

Note that if an init-container is used, any secret mounted into the driver container will also be mounted into the
init-container of the driver. Similarly, any secret mounted into an executor container will also be mounted into the
init-container of the executor.

## Introspection and Debugging

These are the different ways in which you can investigate a running/completed Spark application, monitor progress, and
take actions.

### Accessing Logs

Logs can be accessed using the Kubernetes API and the `kubectl` CLI. When a Spark application is running, it's possible
to stream logs from the application using:

```bash
kubectl -n=<namespace> logs -f <driver-pod-name>
```

The same logs can also be accessed through the
[Kubernetes dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) if installed on
the cluster.

### Accessing Driver UI

The UI associated with any application can be accessed locally using
[`kubectl port-forward`](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/#forward-a-local-port-to-a-port-on-the-pod).

```bash
kubectl port-forward <driver-pod-name> 4040:4040
```

Then, the Spark driver UI can be accessed on `http://localhost:4040`.

### Debugging

There may be several kinds of failures. If the Kubernetes API server rejects the request made from spark-submit, or the
connection is refused for a different reason, the submission logic should indicate the error encountered. However, if there
are errors during the running of the application, often, the best way to investigate may be through the Kubernetes CLI.

To get some basic information about the scheduling decisions made around the driver pod, you can run:

```bash
kubectl describe pod <spark-driver-pod>
```

If the pod has encountered a runtime error, the status can be probed further using:

```bash
kubectl logs <spark-driver-pod>
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
`spark.kubernetes.authenticate.driver.serviceAccountName=<service account name>`. For example to make the driver pod
use the `spark` service account, a user simply adds the following option to the `spark-submit` command:

```
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark
```

To create a custom service account, a user can use the `kubectl create serviceaccount` command. For example, the
following command creates a service account named `spark`:

```bash
kubectl create serviceaccount spark
```

To grant a service account a `Role` or `ClusterRole`, a `RoleBinding` or `ClusterRoleBinding` is needed. To create
a `RoleBinding` or `ClusterRoleBinding`, a user can use the `kubectl create rolebinding` (or `clusterrolebinding`
for `ClusterRoleBinding`) command. For example, the following command creates an `edit` `ClusterRole` in the `default`
namespace and grants it to the `spark` service account created above:

```bash
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```

Note that a `Role` can only be used to grant access to resources (like pods) within a single namespace, whereas a
`ClusterRole` can be used to grant access to cluster-scoped resources (like nodes) as well as namespaced resources
(like pods) across all namespaces. For Spark on Kubernetes, since the driver always creates executor pods in the
same namespace, a `Role` is sufficient, although users may use a `ClusterRole` instead. For more information on
RBAC authorization and how to configure Kubernetes service accounts for pods, please refer to
[Using RBAC Authorization](https://kubernetes.io/docs/admin/authorization/rbac/) and
[Configure Service Accounts for Pods](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/).

## Client Mode

Client mode is not currently supported.

## Future Work

There are several Spark on Kubernetes features that are currently being incubated in a fork -
[apache-spark-on-k8s/spark](https://github.com/apache-spark-on-k8s/spark), which are expected to eventually make it into
future versions of the spark-kubernetes integration.

Some of these include:

* PySpark
* R
* Dynamic Executor Scaling
* Local File Dependency Management
* Spark Application Management
* Job Queues and Resource Management

You can refer to the [documentation](https://apache-spark-on-k8s.github.io/userdocs/) if you want to try these features
and provide feedback to the development team.

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
    a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server when starting the driver. This file
    must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not provide
    a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server when starting the driver. This
    file must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not
    provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.oauthToken</code></td>
  <td>(none)</td>
  <td>
    OAuth token to use when authenticating against the Kubernetes API server when starting the driver. Note
    that unlike the other authentication options, this is expected to be the exact string value of the token to use for
    the authentication.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.submission.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    Path to the OAuth token file containing the token to use when authenticating against the Kubernetes API server when starting the driver.
    This file must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not
    provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.caCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the CA cert file for connecting to the Kubernetes API server over TLS from the driver pod when requesting
    executors. This file must be located on the submitting machine's disk, and will be uploaded to the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server from the driver pod when requesting
    executors. This file must be located on the submitting machine's disk, and will be uploaded to the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme). If this is specified, it is highly
    recommended to set up TLS for the driver submission server, as this value is sensitive information that would be
    passed to the driver pod in plaintext otherwise.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server from the driver pod when
    requesting executors. This file must be located on the submitting machine's disk, and will be uploaded to the
    driver pod. Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.oauthToken</code></td>
  <td>(none)</td>
  <td>
    OAuth token to use when authenticating against the Kubernetes API server from the driver pod when
    requesting executors. Note that unlike the other authentication options, this must be the exact string value of
    the token to use for the authentication. This token value is uploaded to the driver pod. If this is specified, it is
    highly recommended to set up TLS for the driver submission server, as this value is sensitive information that would
    be passed to the driver pod in plaintext otherwise.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    Path to the OAuth token file containing the token to use when authenticating against the Kubernetes API server from the driver pod when
    requesting executors. Note that unlike the other authentication options, this file must contain the exact string value of
    the token to use for the authentication. This token value is uploaded to the driver pod. If this is specified, it is
    highly recommended to set up TLS for the driver submission server, as this value is sensitive information that would
    be passed to the driver pod in plaintext otherwise.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.mounted.caCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the CA cert file for connecting to the Kubernetes API server over TLS from the driver pod when requesting
    executors. This path must be accessible from the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.mounted.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server from the driver pod when requesting
    executors. This path must be accessible from the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.mounted.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server from the driver pod when
    requesting executors. This path must be accessible from the driver pod.
    Specify this as a path as opposed to a URI (i.e. do not provide a scheme).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.mounted.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    Path to the file containing the OAuth token to use when authenticating against the Kubernetes API server from the driver pod when
    requesting executors. This path must be accessible from the driver pod.
    Note that unlike the other authentication options, this file must contain the exact string value of the token to use for the authentication.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.driver.serviceAccountName</code></td>
  <td><code>default</code></td>
  <td>
    Service account that is used when running the driver pod. The driver pod uses this service account when requesting
    executor pods from the API server. Note that this cannot be specified alongside a CA cert file, client key file,
    client cert file, and/or OAuth token.
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
    Name of the driver pod. If not set, the driver pod name is set to "spark.app.name" suffixed by the current timestamp
    to avoid name conflicts.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.podNamePrefix</code></td>
  <td>(none)</td>
  <td>
    Prefix for naming the executor pods.
    If not set, the executor pod name is set to driver pod name suffixed by an integer.
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
    Specify the hard CPU [limit](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container) for the driver pod.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.limit.cores</code></td>
  <td>(none)</td>
  <td>
    Specify the hard CPU [limit](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container) for each executor pod launched for the Spark Application.
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
  <td><code>spark.kubernetes.mountDependencies.jarsDownloadDir</code></td>
  <td><code>/var/spark-data/spark-jars</code></td>
  <td>
    Location to download jars to in the driver and executors.
    This directory must be empty and will be mounted as an empty directory volume on the driver and executor pods.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.mountDependencies.filesDownloadDir</code></td>
  <td><code>/var/spark-data/spark-files</code></td>
  <td>
    Location to download jars to in the driver and executors.
    This directory must be empty and will be mounted as an empty directory volume on the driver and executor pods.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.mountDependencies.timeout</code></td>
  <td>300s</td>
  <td>
   Timeout in seconds before aborting the attempt to download and unpack dependencies from remote locations into
   the driver and executor pods.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.mountDependencies.maxSimultaneousDownloads</code></td>
  <td>5</td>
  <td>
   Maximum number of remote dependencies to download simultaneously in a driver or executor pod.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.initContainer.image</code></td>
  <td><code>(value of spark.kubernetes.container.image)</code></td>
  <td>
   Custom container image for the init container of both driver and executors.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.secrets.[SecretName]</code></td>
  <td>(none)</td>
  <td>
   Add the <a href="https://kubernetes.io/docs/concepts/configuration/secret/">Kubernetes Secret</a> named <code>SecretName</code> to the driver pod on the path specified in the value. For example,
   <code>spark.kubernetes.driver.secrets.spark-secret=/etc/secrets</code>. Note that if an init-container is used,
   the secret will also be added to the init-container in the driver pod.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.secrets.[SecretName]</code></td>
  <td>(none)</td>
  <td>
   Add the <a href="https://kubernetes.io/docs/concepts/configuration/secret/">Kubernetes Secret</a> named <code>SecretName</code> to the executor pod on the path specified in the value. For example,
   <code>spark.kubernetes.executor.secrets.spark-secret=/etc/secrets</code>. Note that if an init-container is used,
   the secret will also be added to the init-container in the executor pod.
  </td>
</tr>
</table>