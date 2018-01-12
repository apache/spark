---
layout: global
title: Running Spark on Kubernetes
---
<<<<<<< HEAD

Support for running on [Kubernetes](https://kubernetes.io/docs/whatisk8s/) is available in experimental status. The
feature set is currently limited and not well-tested. This should not be used in production environments.

## Prerequisites

* You must have a running Kubernetes cluster with access configured to it
using [kubectl](https://kubernetes.io/docs/user-guide/prereqs/). If you do not already have a working Kubernetes
cluster, you may setup a test cluster on your local machine using
[minikube](https://kubernetes.io/docs/getting-started-guides/minikube/).
  * We recommend that minikube be updated to the most recent version (0.19.0 at the time of this documentation), as some
  earlier versions may not start up the kubernetes cluster with all the necessary components.
* You must have appropriate permissions to create and list [pods](https://kubernetes.io/docs/user-guide/pods/),
[ConfigMaps](https://kubernetes.io/docs/tasks/configure-pod-container/configmap/) and
[secrets](https://kubernetes.io/docs/concepts/configuration/secret/) in your cluster. You can verify that
you can list these resources by running `kubectl get pods`, `kubectl get configmap`, and `kubectl get secrets` which
should give you a list of pods and configmaps (if any) respectively.
  * The service account or credentials used by the driver pods must have appropriate permissions
    as well for editing pod spec.
* You must have a spark distribution with Kubernetes support. This may be obtained from the
[release tarball](https://github.com/apache-spark-on-k8s/spark/releases) or by
[building Spark with Kubernetes support](../resource-managers/kubernetes/README.md#building-spark-with-kubernetes-support).
* You must have [Kubernetes DNS](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/) configured in
your cluster.

## Driver & Executor Images

Kubernetes requires users to supply images that can be deployed into containers within pods. The images are built to
be run in a container runtime environment that Kubernetes supports. Docker is a container runtime environment that is
frequently used with Kubernetes, so Spark provides some support for working with Docker to get started quickly.

If you wish to use pre-built docker images, you may use the images published in
[kubespark](https://hub.docker.com/u/kubespark/). The images are as follows:

<table class="table">
<tr><th>Component</th><th>Image</th></tr>
<tr>
  <td>Spark Driver Image</td>
  <td><code>kubespark/spark-driver:v2.2.0-kubernetes-0.3.0</code></td>
</tr>
<tr>
  <td>Spark Executor Image</td>
  <td><code>kubespark/spark-executor:v2.2.0-kubernetes-0.3.0</code></td>
</tr>
<tr>
  <td>Spark Initialization Image</td>
  <td><code>kubespark/spark-init:v2.2.0-kubernetes-0.3.0</code></td>
</tr>
</table>

You may also build these docker images from sources, or customize them as required. Spark distributions include the
Docker files for the base-image, driver, executor, and init-container at `dockerfiles/spark-base/Dockerfile`, `dockerfiles/driver/Dockerfile`,
`dockerfiles/executor/Dockerfile`, and `dockerfiles/init-container/Dockerfile` respectively. Use these Docker files to
build the Docker images, and then tag them with the registry that the images should be sent to. Finally, push the images
to the registry.

For example, if the registry host is `registry-host` and the registry is listening on port 5000:

    cd $SPARK_HOME
    docker build -t registry-host:5000/spark-base:latest -f dockerfiles/spark-base/Dockerfile .
    docker build -t registry-host:5000/spark-driver:latest -f dockerfiles/driver/Dockerfile .
    docker build -t registry-host:5000/spark-executor:latest -f dockerfiles/executor/Dockerfile .
    docker build -t registry-host:5000/spark-init:latest -f dockerfiles/init-container/Dockerfile .
    docker push registry-host:5000/spark-base:latest
    docker push registry-host:5000/spark-driver:latest
    docker push registry-host:5000/spark-executor:latest
    docker push registry-host:5000/spark-init:latest
    
Note that `spark-base` is the base image for the other images.  It must be built first before the other images, and then afterwards the other images can be built in any order.

## Submitting Applications to Kubernetes

Kubernetes applications can be executed via `spark-submit`. For example, to compute the value of pi, assuming the images
are set up as described above:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.initcontainer.docker.image=kubespark/spark-init:v2.2.0-kubernetes-0.3.0 \
      local:///opt/spark/examples/jars/spark-examples_2.11-2.2.0-k8s-0.3.0.jar
=======
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
>>>>>>> master

The Spark master, specified either via passing the `--master` command line argument to `spark-submit` or by setting
`spark.master` in the application's configuration, must be a URL with the format `k8s://<api_server_url>`. Prefixing the
master string with `k8s://` will cause the Spark application to launch on the Kubernetes cluster, with the API server
being contacted at `api_server_url`. If no HTTP protocol is specified in the URL, it defaults to `https`. For example,
setting the master to `k8s://example.com:443` is equivalent to setting it to `k8s://https://example.com:443`, but to
<<<<<<< HEAD
connect without TLS on a different port, the master would be set to `k8s://http://example.com:8443`.

If you have a Kubernetes cluster setup, one way to discover the apiserver URL is by executing `kubectl cluster-info`.

    > kubectl cluster-info
    Kubernetes master is running at http://127.0.0.1:8080

In the above example, the specific Kubernetes cluster can be used with spark submit by specifying
`--master k8s://http://127.0.0.1:8080` as an argument to spark-submit.

Note that applications can currently only be executed in cluster mode, where the driver and its executors are running on
the cluster.

Finally, notice that in the above example we specify a jar with a specific URI with a scheme of `local://`. This URI is
the location of the example jar that is already in the Docker image. Using dependencies that are on your machine's local
disk is discussed below.

When Kubernetes [RBAC](https://kubernetes.io/docs/admin/authorization/rbac/) is enabled,
the `default` service account used by the driver may not have appropriate pod `edit` permissions
for launching executor pods. We recommend to add another service account, say `spark`, with
the necessary privilege. For example:

    kubectl create serviceaccount spark
    kubectl create clusterrolebinding spark-edit --clusterrole edit  \
        --serviceaccount default:spark --namespace default

With this, one can add `--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark` to
the spark-submit command line above to specify the service account to use.

## Dependency Management

Application dependencies that are being submitted from your machine need to be sent to a **resource staging server**
that the driver and executor can then communicate with to retrieve those dependencies. A YAML file denoting a minimal
set of Kubernetes resources that runs this service is located in the file `conf/kubernetes-resource-staging-server.yaml`.
This YAML file configures a Deployment with one pod running the resource staging server configured with a ConfigMap,
and exposes the server through a Service with a fixed NodePort. Deploying a resource staging server with the included
YAML file requires you to have permissions to create Deployments, Services, and ConfigMaps.

To run the resource staging server with default configurations, the Kubernetes resources can be created:

    kubectl create -f conf/kubernetes-resource-staging-server.yaml

and then you can compute the value of Pi as follows:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://<k8s-apiserver-host>:<k8s-apiserver-port> \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.initcontainer.docker.image=kubespark/spark-init:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.resourceStagingServer.uri=http://<address-of-any-cluster-node>:31000 \
      examples/jars/spark-examples_2.11-2.2.0-k8s-0.3.0.jar

The Docker image for the resource staging server may also be built from source, in a similar manner to the driver
and executor images. The Dockerfile is provided in `dockerfiles/resource-staging-server/Dockerfile`.

The provided YAML file specifically sets the NodePort to 31000 on the service's specification. If port 31000 is not
available on any of the nodes of your cluster, you should remove the NodePort field from the service's specification
and allow the Kubernetes cluster to determine the NodePort itself. Be sure to provide the correct port in the resource
staging server URI when submitting your application, in accordance to the NodePort chosen by the Kubernetes cluster.

### Dependency Management Without The Resource Staging Server

Note that this resource staging server is only required for submitting local dependencies. If your application's
dependencies are all hosted in remote locations like HDFS or http servers, they may be referred to by their appropriate
remote URIs. Also, application dependencies can be pre-mounted into custom-built Docker images. Those dependencies
can be added to the classpath by referencing them with `local://` URIs and/or setting the `SPARK_EXTRA_CLASSPATH`
environment variable in your Dockerfiles.

### Accessing Kubernetes Clusters

For details about running on public cloud environments, such as Google Container Engine (GKE), refer to [running Spark in the cloud with Kubernetes](running-on-kubernetes-cloud.md).

Spark-submit also supports submission through the
[local kubectl proxy](https://kubernetes.io/docs/user-guide/accessing-the-cluster/#using-kubectl-proxy). One can use the
authenticating proxy to communicate with the api server directly without passing credentials to spark-submit.

The local proxy can be started by running:

    kubectl proxy

If our local proxy were listening on port 8001, we would have our submission looking like the following:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://http://127.0.0.1:8001 \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.initcontainer.docker.image=kubespark/spark-init:v2.2.0-kubernetes-0.3.0 \
      local:///opt/spark/examples/jars/spark-examples_2.11-2.2.0-k8s-0.3.0.jar

Communication between Spark and Kubernetes clusters is performed using the fabric8 kubernetes-client library.
The above mechanism using `kubectl proxy` can be used when we have authentication providers that the fabric8
kubernetes-client library does not support. Authentication using X509 Client Certs and OAuth tokens
is currently supported.

### Running PySpark

Running PySpark on Kubernetes leverages the same spark-submit logic when launching on Yarn and Mesos. 
Python files can be distributed by including, in the conf, `--py-files` 

Below is an example submission: 


```
    bin/spark-submit \
      --deploy-mode cluster \
      --master k8s://http://127.0.0.1:8001 \
      --kubernetes-namespace default \
      --conf spark.executor.memory=500m \
      --conf spark.driver.memory=1G \
      --conf spark.driver.cores=1 \
      --conf spark.executor.cores=1 \
      --conf spark.executor.instances=1 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=spark-driver-py:latest \
      --conf spark.kubernetes.executor.docker.image=spark-executor-py:latest \
      --conf spark.kubernetes.initcontainer.docker.image=spark-init:latest \
      --py-files local:///opt/spark/examples/src/main/python/sort.py \
      local:///opt/spark/examples/src/main/python/pi.py 100
```

## Dynamic Allocation in Kubernetes

Spark on Kubernetes supports Dynamic Allocation with cluster mode. This mode requires running
an external shuffle service. This is typically a [daemonset](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/)
with a provisioned [hostpath](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath) volume.
This shuffle service may be shared by executors belonging to different SparkJobs. Using Spark with dynamic allocation
on Kubernetes assumes that a cluster administrator has set up one or more shuffle-service daemonsets in the cluster.

A sample configuration file is provided in `conf/kubernetes-shuffle-service.yaml` which can be customized as needed
for a particular cluster. It is important to note that `spec.template.metadata.labels` are setup appropriately for the shuffle
service because there may be multiple shuffle service instances running in a cluster. The labels give Spark applications
a way to target a particular shuffle service.

For example, if the shuffle service we want to use is in the default namespace, and
has pods with labels `app=spark-shuffle-service` and `spark-version=2.2.0`, we can
use those tags to target that particular shuffle service at job launch time. In order to run a job with dynamic allocation enabled,
the command may then look like the following:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.GroupByTest \
      --master k8s://<k8s-master>:<port> \
      --kubernetes-namespace default \
      --conf spark.local.dir=/tmp/spark-local
      --conf spark.app.name=group-by-test \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:latest \
      --conf spark.dynamicAllocation.enabled=true \
      --conf spark.shuffle.service.enabled=true \
      --conf spark.kubernetes.shuffle.namespace=default \
      --conf spark.kubernetes.shuffle.labels="app=spark-shuffle-service,spark-version=2.2.0" \
      local:///opt/spark/examples/jars/spark-examples_2.11-2.2.0-k8s-0.3.0.jar 10 400000 2

The external shuffle service has to mount directories that can be shared with the executor pods. The provided example
YAML spec mounts a hostPath volume to the external shuffle service pods, but these hostPath volumes must also be mounted
into the executors. When using the external shuffle service, the directories specified in the `spark.local.dir`
configuration are mounted as hostPath volumes into all of the executor containers. To ensure that one does not
accidentally mount the incorrect hostPath volumes, the value of `spark.local.dir` must be specified in your
application's configuration when using Kubernetes, even though it defaults to the JVM's temporary directory when using
other cluster managers.

## Advanced

### Securing the Resource Staging Server with TLS

The default configuration of the resource staging server is not secured with TLS. It is highly recommended to configure
this to protect the secrets and jars/files being submitted through the staging server.

The YAML file in `conf/kubernetes-resource-staging-server.yaml` includes a ConfigMap resource that holds the resource
staging server's configuration. The properties can be adjusted here to make the resource staging server listen over TLS.
Refer to the [security](security.html) page for the available settings related to TLS. The namespace for the
resource staging server is `kubernetes.resourceStagingServer`, so for example the path to the server's keyStore would
be set by `spark.ssl.kubernetes.resourceStagingServer.keyStore`.

In addition to the settings specified by the previously linked security page, the resource staging server supports the
following additional configurations:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.keyPem</code></td>
  <td>(none)</td>
  <td>
    Private key file encoded in PEM format that the resource staging server uses to secure connections over TLS. If this
    is specified, the associated public key file must be specified in
    <code>spark.ssl.kubernetes.resourceStagingServer.serverCertPem</code>. PEM files and a keyStore file (set by
    <code>spark.ssl.kubernetes.resourceStagingServer.keyStore</code>) cannot both be specified at the same time.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.serverCertPem</code></td>
  <td>(none)</td>
  <td>
    Certificate file encoded in PEM format that the resource staging server uses to secure connections over TLS. If this
    is specified, the associated private key file must be specified in
    <code>spark.ssl.kubernetes.resourceStagingServer.keyPem</code>. PEM files and a keyStore file (set by
    <code>spark.ssl.kubernetes.resourceStagingServer.keyStore</code>) cannot both be specified at the same time.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.keyStorePasswordFile</code></td>
  <td>(none)</td>
  <td>
    Provides the KeyStore password through a file in the container instead of a static value. This is useful if the
    keyStore's password is to be mounted into the container with a secret.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.keyPasswordFile</code></td>
  <td>(none)</td>
  <td>
    Provides the keyStore's key password using a file in the container instead of a static value. This is useful if the
    keyStore's key password is to be mounted into the container with a secret.
  </td>
</tr>
</table>

Note that while the properties can be set in the ConfigMap, you will still need to consider the means of mounting the
appropriate secret files into the resource staging server's container. A common mechanism that is used for this is
to use [Kubernetes secrets](https://kubernetes.io/docs/concepts/configuration/secret/) that are mounted as secret
volumes. Refer to the appropriate Kubernetes documentation for guidance and adjust the resource staging server's
specification in the provided YAML file accordingly.

Finally, when you submit your application, you must specify either a trustStore or a PEM-encoded certificate file to
communicate with the resource staging server over TLS. The trustStore can be set with
`spark.ssl.kubernetes.resourceStagingServer.trustStore`, or a certificate file can be set with
`spark.ssl.kubernetes.resourceStagingServer.clientCertPem`. For example, our SparkPi example now looks like this:

    bin/spark-submit \
      --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi \
      --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
      --kubernetes-namespace default \
      --conf spark.executor.instances=5 \
      --conf spark.app.name=spark-pi \
      --conf spark.kubernetes.driver.docker.image=kubespark/spark-driver:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.executor.docker.image=kubespark/spark-executor:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.initcontainer.docker.image=kubespark/spark-init:v2.2.0-kubernetes-0.3.0 \
      --conf spark.kubernetes.resourceStagingServer.uri=https://<address-of-any-cluster-node>:31000 \
      --conf spark.ssl.kubernetes.resourceStagingServer.enabled=true \
      --conf spark.ssl.kubernetes.resourceStagingServer.clientCertPem=/home/myuser/cert.pem \
      examples/jars/spark-examples_2.11-2.2.0-k8s-0.3.0.jar

### Spark Properties

Below are some other common properties that are specific to Kubernetes. Most of the other configurations are the same
from the other deployment modes. See the [configuration page](configuration.html) for more information on those.
=======
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
`SPARK_EXTRA_CLASSPATH` environment variable in your Dockerfiles.

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
>>>>>>> master

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.kubernetes.namespace</code></td>
  <td><code>default</code></td>
  <td>
<<<<<<< HEAD
    The namespace that will be used for running the driver and executor pods. When using
    <code>spark-submit</code> in cluster mode, this can also be passed to <code>spark-submit</code> via the
    <code>--kubernetes-namespace</code> command line argument.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.docker.image</code></td>
  <td><code>spark-driver:2.2.0</code></td>
  <td>
    Docker image to use for the driver. Specify this using the standard
    <a href="https://docs.docker.com/engine/reference/commandline/tag/">Docker tag</a> format.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.docker.image</code></td>
  <td><code>spark-executor:2.2.0</code></td>
  <td>
    Docker image to use for the executors. Specify this using the standard
    <a href="https://docs.docker.com/engine/reference/commandline/tag/">Docker tag</a> format.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.initcontainer.docker.image</code></td>
  <td><code>spark-init:2.2.0</code></td>
  <td>
    Docker image to use for the init-container that is run before the driver and executor containers. Specify this using
    the standard <a href="https://docs.docker.com/engine/reference/commandline/tag/">Docker tag</a> format. The
    init-container is responsible for fetching application dependencies from both remote locations like HDFS or S3,
    and from the resource staging server, if applicable.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.shuffle.namespace</code></td>
  <td><code>default</code></td>
  <td>
    Namespace in which the shuffle service pods are present. The shuffle service must be
    created in the cluster prior to attempts to use it.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.shuffle.labels</code></td>
  <td>(none)</td>
  <td>
    Labels that will be used to look up shuffle service pods. This should be a comma-separated list of label key-value pairs,
    where each label is in the format <code>key=value</code>. The labels chosen must be such that
    they match exactly one shuffle service pod on each node that executors are launched.
=======
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
>>>>>>> master
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
<<<<<<< HEAD
  <td><code>1</code></td>
  <td>
    Number of seconds to wait between each round of executor pod allocation.
=======
  <td><code>1s</code></td>
  <td>
    Time to wait between each round of executor pod allocation. Specifying values less than 1 second may lead to
    excessive CPU usage on the spark driver.
>>>>>>> master
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
<<<<<<< HEAD
=======
  <td><code>spark.kubernetes.authenticate.submission.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    Path to the OAuth token file containing the token to use when authenticating against the Kubernetes API server when starting the driver.
    This file must be located on the submitting machine's disk. Specify this as a path as opposed to a URI (i.e. do not
    provide a scheme).
  </td>
</tr>
<tr>
>>>>>>> master
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
<<<<<<< HEAD
    OAuth token to use when authenticating against the against the Kubernetes API server from the driver pod when
=======
    OAuth token to use when authenticating against the Kubernetes API server from the driver pod when
>>>>>>> master
    requesting executors. Note that unlike the other authentication options, this must be the exact string value of
    the token to use for the authentication. This token value is uploaded to the driver pod. If this is specified, it is
    highly recommended to set up TLS for the driver submission server, as this value is sensitive information that would
    be passed to the driver pod in plaintext otherwise.
  </td>
</tr>
<tr>
<<<<<<< HEAD
  <td><code>spark.kubernetes.authenticate.driver.serviceAccountName</code></td>
  <td><code>default</code></td>
  <td>
    Service account that is used when running the driver pod. The driver pod uses this service account when requesting
    executor pods from the API server. Note that this cannot be specified alongside a CA cert file, client key file,
    client cert file, and/or OAuth token.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.resourceStagingServer.caCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the CA cert file for connecting to the Kubernetes API server over TLS from the resource staging server when
    it monitors objects in determining when to clean up resource bundles.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.resourceStagingServer.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client key file for authenticating against the Kubernetes API server from the resource staging server
    when it monitors objects in determining when to clean up resource bundles. The resource staging server must have
    credentials that allow it to view API objects in any namespace.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.resourceStagingServer.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Path to the client cert file for authenticating against the Kubernetes API server from the resource staging server
    when it monitors objects in determining when to clean up resource bundles. The resource staging server must have
    credentials that allow it to view API objects in any namespace.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.resourceStagingServer.oauthToken</code></td>
  <td>(none)</td>
  <td>
    OAuth token value for authenticating against the Kubernetes API server from the resource staging server
    when it monitors objects in determining when to clean up resource bundles. The resource staging server must have
    credentials that allow it to view API objects in any namespace. Note that this cannot be set at the same time as
    <code>spark.kubernetes.authenticate.resourceStagingServer.oauthTokenFile</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.resourceStagingServer.oauthTokenFile</code></td>
  <td>(none)</td>
  <td>
    File containing the OAuth token to use when authenticating against the against the Kubernetes API server from the
    resource staging server, when it monitors objects in determining when to clean up resource bundles. The resource
    staging server must have credentials that allow it to view API objects in any namespace. Note that this cannot be
    set at the same time as <code>spark.kubernetes.authenticate.resourceStagingServer.oauthToken</code>.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.authenticate.resourceStagingServer.useServiceAccountCredentials</code></td>
  <td>true</td>
  <td>
    Whether or not to use a service account token and a service account CA certificate when the resource staging server
    authenticates to Kubernetes. If this is set, interactions with Kubernetes will authenticate using a token located at
    <code>/var/run/secrets/kubernetes.io/serviceaccount/token</code> and the CA certificate located at
    <code>/var/run/secrets/kubernetes.io/serviceaccount/ca.crt</code>. Note that if
    <code>spark.kubernetes.authenticate.resourceStagingServer.oauthTokenFile</code> is set, it takes precedence
    over the usage of the service account token file. Also, if
    <code>spark.kubernetes.authenticate.resourceStagingServer.caCertFile</code> is set, it takes precedence over using
    the service account's CA certificate file. This generally should be set to true (the default value) when the
    resource staging server is deployed as a Kubernetes pod, but should be set to false if the resource staging server
    is deployed by other means (i.e. when running the staging server process outside of Kubernetes). The resource
    staging server must have credentials that allow it to view API objects in any namespace.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.memoryOverhead</code></td>
  <td>executorMemory * 0.10, with minimum of 384</td>
  <td>
    The amount of off-heap memory (in megabytes) to be allocated per executor. This is memory that accounts for things
    like VM overheads, interned strings, other native overheads, etc. This tends to grow with the executor size
    (typically 6-10%).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.label.[labelKey]</code></td>
  <td>(none)</td>
  <td>
    Adds a label to the driver pod, with key <code>labelKey</code> and the value as the configuration's value. For
    example, setting <code>spark.kubernetes.driver.label.identifier</code> to <code>myIdentifier</code> will result in
    the driver pod having a label with key <code>identifier</code> and value <code>myIdentifier</code>. Multiple labels
    can be added by setting multiple configurations with this prefix.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.annotation.[annotationKey]</code></td>
  <td>(none)</td>
  <td>
    Adds an annotation to the driver pod, with key <code>annotationKey</code> and the value as the configuration's
    value. For example, setting <code>spark.kubernetes.driver.annotation.identifier</code> to <code>myIdentifier</code>
    will result in the driver pod having an annotation with key <code>identifier</code> and value
    <code>myIdentifier</code>. Multiple annotations can be added by setting multiple configurations with this prefix.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.label.[labelKey]</code></td>
  <td>(none)</td>
  <td>
    Adds a label to all executor pods, with key <code>labelKey</code> and the value as the configuration's value. For
    example, setting <code>spark.kubernetes.executor.label.identifier</code> to <code>myIdentifier</code> will result in
    the executor pods having a label with key <code>identifier</code> and value <code>myIdentifier</code>. Multiple
    labels can be added by setting multiple configurations with this prefix.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.annotation.[annotationKey]</code></td>
  <td>(none)</td>
  <td>
    Adds an annotation to the executor pods, with key <code>annotationKey</code> and the value as the configuration's
    value. For example, setting <code>spark.kubernetes.executor.annotation.identifier</code> to <code>myIdentifier</code>
    will result in the executor pods having an annotation with key <code>identifier</code> and value
    <code>myIdentifier</code>. Multiple annotations can be added by setting multiple configurations with this prefix.
=======
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
>>>>>>> master
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
<<<<<<< HEAD
  <td><code>spark.kubernetes.submission.waitAppCompletion</code></td>
  <td><code>true</code></td>
  <td>
    In cluster mode, whether to wait for the application to finish before exiting the launcher process.  When changed to
    false, the launcher has a "fire-and-forget" behavior when launching the Spark job.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.resourceStagingServer.port</code></td>
  <td><code>10000</code></td>
  <td>
    Port for the resource staging server to listen on when it is deployed.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.resourceStagingServer.uri</code></td>
  <td>(none)</td>
  <td>
    URI of the resource staging server that Spark should use to distribute the application's local dependencies. Note
    that by default, this URI must be reachable by both the submitting machine and the pods running in the cluster. If
    one URI is not simultaneously reachable both by the submitter and the driver/executor pods, configure the pods to
    access the staging server at a different URI by setting
    <code>spark.kubernetes.resourceStagingServer.internal.uri</code> as discussed below.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.resourceStagingServer.internal.uri</code></td>
  <td>Value of <code>spark.kubernetes.resourceStagingServer.uri</code></td>
  <td>
    URI of the resource staging server to communicate with when init-containers bootstrap the driver and executor pods
    with submitted local dependencies. Note that this URI must by the pods running in the cluster. This is useful to
    set if the resource staging server has a separate "internal" URI that must be accessed by components running in the
    cluster.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.internal.trustStore</code></td>
  <td>Value of <code>spark.ssl.kubernetes.resourceStagingServer.trustStore</code></td>
  <td>
    Location of the trustStore file to use when communicating with the resource staging server over TLS, as
    init-containers bootstrap the driver and executor pods with submitted local dependencies. This can be a URI with a
    scheme of <code>local://</code>, which denotes that the file is pre-mounted on the pod's disk. A uri without a
    scheme or a scheme of <code>file://</code> will result in this file being mounted from the submitting machine's
    disk as a secret into the init-containers.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.internal.trustStorePassword</code></td>
  <td>Value of <code><code>spark.ssl.kubernetes.resourceStagingServer.trustStorePassword</code></td>
  <td>
    Password of the trustStore file that is used when communicating with the resource staging server over TLS, as
    init-containers bootstrap the driver and executor pods with submitted local dependencies.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.internal.trustStoreType</code></td>
  <td>Value of <code><code>spark.ssl.kubernetes.resourceStagingServer.trustStoreType</code></td>
  <td>
    Type of the trustStore file that is used when communicating with the resource staging server over TLS, when
    init-containers bootstrap the driver and executor pods with submitted local dependencies.
  </td>
</tr>
<tr>
  <td><code>spark.ssl.kubernetes.resourceStagingServer.internal.clientCertPem</code></td>
  <td>Value of <code>spark.ssl.kubernetes.resourceStagingServer.clientCertPem</code></td>
  <td>
    Location of the certificate file to use when communicating with the resource staging server over TLS, as
    init-containers bootstrap the driver and executor pods with submitted local dependencies. This can be a URI with a
    scheme of <code>local://</code>, which denotes that the file is pre-mounted on the pod's disk. A uri without a
    scheme or a scheme of <code>file://</code> will result in this file being mounted from the submitting machine's
    disk as a secret into the init-containers.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.mountdependencies.jarsDownloadDir</code></td>
  <td><code>/var/spark-data/spark-jars</code></td>
  <td>
    Location to download jars to in the driver and executors. This will be mounted as an empty directory volume
    into the driver and executor containers.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.mountdependencies.filesDownloadDir</code></td>
  <td><code>/var/spark-data/spark-files</code></td>
  <td>
    Location to download files to in the driver and executors. This will be mounted as an empty directory volume
    into the driver and executor containers.
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
  <td><code>spark.kubernetes.docker.image.pullPolicy</code></td>
  <td><code>IfNotPresent</code></td>
  <td>
    Docker image pull policy used when pulling Docker images with Kubernetes.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.limit.cores</code></td>
  <td>(none)</td>
  <td>
    Specify the hard cpu limit for the driver pod
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.limit.cores</code></td>
  <td>(none)</td>
  <td>
    Specify the hard cpu limit for a single executor pod
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
  <td><code>spark.executorEnv.[EnvironmentVariableName]</code></td> 
  <td>(none)</td>
  <td>
    Add the environment variable specified by <code>EnvironmentVariableName</code> to
    the Executor process. The user can specify multiple of these to set multiple environment variables.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driverEnv.[EnvironmentVariableName]</code></td> 
  <td>(none)</td>
  <td>
    Add the environment variable specified by <code>EnvironmentVariableName</code> to
    the Driver process. The user can specify multiple of these to set multiple environment variables.
=======
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
>>>>>>> master
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.secrets.[SecretName]</code></td>
  <td>(none)</td>
  <td>
<<<<<<< HEAD
    Mounts the Kubernetes secret named <code>SecretName</code> onto the path specified by the value
    in the driver Pod. The user can specify multiple instances of this for multiple secrets.
=======
   Add the <a href="https://kubernetes.io/docs/concepts/configuration/secret/">Kubernetes Secret</a> named <code>SecretName</code> to the driver pod on the path specified in the value. For example,
   <code>spark.kubernetes.driver.secrets.spark-secret=/etc/secrets</code>. Note that if an init-container is used,
   the secret will also be added to the init-container in the driver pod.
>>>>>>> master
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.secrets.[SecretName]</code></td>
  <td>(none)</td>
  <td>
<<<<<<< HEAD
    Mounts the Kubernetes secret named <code>SecretName</code> onto the path specified by the value
    in the executor Pods. The user can specify multiple instances of this for multiple secrets.
  </td>
</tr>
</table>


## Current Limitations

Running Spark on Kubernetes is currently an experimental feature. Some restrictions on the current implementation that
should be lifted in the future include:
* Applications can only run in cluster mode.
* Only Scala and Java applications can be run.
=======
   Add the <a href="https://kubernetes.io/docs/concepts/configuration/secret/">Kubernetes Secret</a> named <code>SecretName</code> to the executor pod on the path specified in the value. For example,
   <code>spark.kubernetes.executor.secrets.spark-secret=/etc/secrets</code>. Note that if an init-container is used,
   the secret will also be added to the init-container in the executor pod.
  </td>
</tr>
</table>
>>>>>>> master
