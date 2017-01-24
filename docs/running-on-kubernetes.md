---
layout: global
title: Running Spark on Kubernetes
---

Support for running on [Kubernetes](https://kubernetes.io/) is available in experimental status. The feature set is
currently limited and not well-tested. This should not be used in production environments.

## Setting Up Docker Images

Kubernetes requires users to supply images that can be deployed into containers within pods. The images are built to
be run in a container runtime environment that Kubernetes supports. Docker is a container runtime environment that is
frequently used with Kubernetes, so Spark provides some support for working with Docker to get started quickly.

To use Spark on Kubernetes with Docker, images for the driver and the executors need to built and published to an
accessible Docker registry. Spark distributions include the Docker files for the driver and the executor at
`dockerfiles/driver/Dockerfile` and `docker/executor/Dockerfile`, respectively. Use these Docker files to build the
Docker images, and then tag them with the registry that the images should be sent to. Finally, push the images to the
registry.

For example, if the registry host is `registry-host` and the registry is listening on port 5000:

    cd $SPARK_HOME
    docker build -t registry-host:5000/spark-driver:latest -f dockerfiles/driver/Dockerfile .
    docker build -t registry-host:5000/spark-executor:latest -f dockerfiles/executor/Dockerfile .
    docker push registry-host:5000/spark-driver:latest
    docker push registry-host:5000/spark-executor:latest
    
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
      --conf spark.kubernetes.driver.docker.image=registry-host:5000/spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=registry-host:5000/spark-executor:latest \
      examples/jars/spark_examples_2.11-2.2.0.jar

The Spark master, specified either via passing the `--master` command line argument to `spark-submit` or by setting
`spark.master` in the application's configuration, must be a URL with the format `k8s://<api_server_url>`. Prefixing the
master string with `k8s://` will cause the Spark application to launch on the Kubernetes cluster, with the API server
being contacted at `api_server_url`. If no HTTP protocol is specified in the URL, it defaults to `https`. For example,
setting the master to `k8s://example.com:443` is equivalent to setting it to `k8s://https://example.com:443`, but to
connect without SSL on a different port, the master would be set to `k8s://http://example.com:8443`.

Note that applications can currently only be executed in cluster mode, where the driver and its executors are running on
the cluster.
 
### Adding Other JARs
 
Spark allows users to provide dependencies that are bundled into the driver's Docker image, or that are on the local
disk of the submitter's machine. These two types of dependencies are specified via different configuration options to
`spark-submit`:
 
* Local jars provided by specifying the `--jars` command line argument to `spark-submit`, or by setting `spark.jars` in
  the application's configuration, will be treated as jars that are located on the *disk of the driver container*. This
  only applies to jar paths that do not specify a scheme or that have the scheme `file://`. Paths with other schemes are
  fetched from their appropriate locations.
* Local jars provided by specifying the `--upload-jars` command line argument to `spark-submit`, or by setting
  `spark.kubernetes.driver.uploads.jars` in the application's configuration, will be treated as jars that are located on
  the *disk of the submitting machine*. These jars are uploaded to the driver docker container before executing the
  application.
* A main application resource path that does not have a scheme or that has the scheme `file://` is assumed to be on the
  *disk of the submitting machine*. This resource is uploaded to the driver docker container before executing the
  application. A remote path can still be specified and the resource will be fetched from the appropriate location.
* A main application resource path that has the scheme `container://` is assumed to be on the *disk of the driver
  container*.
  
In all of these cases, the jars are placed on the driver's classpath, and are also sent to the executors. Below are some
examples of providing application dependencies.

To submit an application with both the main resource and two other jars living on the submitting user's machine:

    bin/spark-submit \
      --deploy-mode cluster \
      --class com.example.applications.SampleApplication \
      --master k8s://192.168.99.100 \
      --upload-jars /home/exampleuser/exampleapplication/dep1.jar,/home/exampleuser/exampleapplication/dep2.jar \
      --conf spark.kubernetes.driver.docker.image=registry-host:5000/spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=registry-host:5000/spark-executor:latest \
      /home/exampleuser/exampleapplication/main.jar
      
Note that since passing the jars through the `--upload-jars` command line argument is equivalent to setting the
`spark.kubernetes.driver.uploads.jars` Spark property, the above will behave identically to this command:

    bin/spark-submit \
      --deploy-mode cluster \
      --class com.example.applications.SampleApplication \
      --master k8s://192.168.99.100 \
      --conf spark.kubernetes.driver.uploads.jars=/home/exampleuser/exampleapplication/dep1.jar,/home/exampleuser/exampleapplication/dep2.jar \
      --conf spark.kubernetes.driver.docker.image=registry-host:5000/spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=registry-host:5000/spark-executor:latest \
      /home/exampleuser/exampleapplication/main.jar

To specify a main application resource that can be downloaded from an HTTP service, and if a plugin for that application
is located in the jar `/opt/spark-plugins/app-plugin.jar` on the docker image's disk:

    bin/spark-submit \
      --deploy-mode cluster \
      --class com.example.applications.PluggableApplication \
      --master k8s://192.168.99.100 \
      --jars /opt/spark-plugins/app-plugin.jar \
      --conf spark.kubernetes.driver.docker.image=registry-host:5000/spark-driver-custom:latest \
      --conf spark.kubernetes.executor.docker.image=registry-host:5000/spark-executor:latest \
      http://example.com:8080/applications/sparkpluggable/app.jar
      
Note that since passing the jars through the `--jars` command line argument is equivalent to setting the `spark.jars`
Spark property, the above will behave identically to this command:

    bin/spark-submit \
      --deploy-mode cluster \
      --class com.example.applications.PluggableApplication \
      --master k8s://192.168.99.100 \
      --conf spark.jars=file:///opt/spark-plugins/app-plugin.jar \
      --conf spark.kubernetes.driver.docker.image=registry-host:5000/spark-driver-custom:latest \
      --conf spark.kubernetes.executor.docker.image=registry-host:5000/spark-executor:latest \
      http://example.com:8080/applications/sparkpluggable/app.jar
      
To specify a main application resource that is in the Docker image, and if it has no other dependencies:

    bin/spark-submit \
      --deploy-mode cluster \
      --class com.example.applications.PluggableApplication \
      --master k8s://192.168.99.100:8443 \
      --conf spark.kubernetes.driver.docker.image=registry-host:5000/spark-driver-custom:latest \
      --conf spark.kubernetes.executor.docker.image=registry-host:5000/spark-executor:latest \
      container:///home/applications/examples/example.jar

### Spark Properties

Below are some other common properties that are specific to Kubernetes. Most of the other configurations are the same
from the other deployment modes. See the [configuration page](configuration.html) for more information on those.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.kubernetes.namespace</code></td>
  <td><code>default</code></td>
  <td>
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
  <td><code>spark.kubernetes.submit.caCertFile</code></td>
  <td>(none)</td>
  <td>
    CA cert file for connecting to Kubernetes over SSL. This file should be located on the submitting machine's disk.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submit.clientKeyFile</code></td>
  <td>(none)</td>
  <td>
    Client key file for authenticating against the Kubernetes API server. This file should be located on the submitting
    machine's disk.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submit.clientCertFile</code></td>
  <td>(none)</td>
  <td>
    Client cert file for authenticating against the Kubernetes API server. This file should be located on the submitting
    machine's disk.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.submit.serviceAccountName</code></td>
  <td><code>default</code></td>
  <td>
    Service account that is used when running the driver pod. The driver pod uses this service account when requesting
    executor pods from the API server.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.uploads.jars</code></td>
  <td>(none)</td>
  <td>
    Comma-separated list of jars to sent to the driver and all executors when submitting the application in cluster
    mode. Refer to <a href="running-on-kubernetes.html#adding-other-jars">adding other jars</a> for more information.
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.executor.memoryOverhead</code></td>
  <td>executorMemory * 0.10, with minimum of 384 </td>
  <td>
    The amount of off-heap memory (in megabytes) to be allocated per executor. This is memory that accounts for things
    like VM overheads, interned strings, other native overheads, etc. This tends to grow with the executor size
    (typically 6-10%).
  </td>
</tr>
<tr>
  <td><code>spark.kubernetes.driver.labels</code></td>
  <td>(none)</td>
  <td>
    Custom labels that will be added to the driver pod. This should be a comma-separated list of label key-value pairs,
    where each label is in the format <code>key=value</code>.
  </td>
</tr>
</table>

## Current Limitations

Running Spark on Kubernetes is currently an experimental feature. Some restrictions on the current implementation that
should be lifted in the future include:
* Applications can only use a fixed number of executors. Dynamic allocation is not supported.
* Applications can only run in cluster mode.
* Only Scala and Java applications can be run.
