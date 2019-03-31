---
layout: global
title: Spark on Kubernetes Integration Tests
---

# Running the Kubernetes Integration Tests

Note that the integration test framework is currently being heavily revised and
is subject to change. Note that currently the integration tests only run with Java 8.

The simplest way to run the integration tests is to install and run Minikube, then run the following from this
directory:

    dev/dev-run-integration-tests.sh

The minimum tested version of Minikube is 0.23.0. The kube-dns addon must be enabled. Minikube should
run with a minimum of 4 CPUs and 6G of memory:

    minikube start --cpus 4 --memory 6144

You can download Minikube [here](https://github.com/kubernetes/minikube/releases).

# Integration test customization

Configuration of the integration test runtime is done through passing different arguments to the test script. 
The main useful options are outlined below.

## Using a different backend

The integration test backend i.e. the K8S cluster used for testing is controlled by the `--deploy-mode` option.  By 
default this is set to `minikube`, the available backends are their prerequisites are as follows.  

### `minikube`

Uses the local `minikube` cluster, this requires that `minikube` 0.23.0 or greater be installed and that it be allocated 
at least 4 CPUs and 6GB memory (some users have reported success with as few as 3 CPUs and 4GB memory).  The tests will 
check if `minikube` is started and abort early if it isn't currently running.

### `docker-for-desktop`

Since July 2018 Docker for Desktop provide an optional Kubernetes cluster that can be enabled as described in this 
[blog post](https://blog.docker.com/2018/07/kubernetes-is-now-available-in-docker-desktop-stable-channel/).  Assuming 
this is enabled using this backend will auto-configure itself from the `docker-for-desktop` context that Docker creates 
in your `~/.kube/config` file. If your config file is in a different location you should set the `KUBECONFIG` 
environment variable appropriately.

### `cloud` 

The cloud backend configures the tests to use an arbitrary Kubernetes cluster running in the cloud or otherwise.

The `cloud` backend auto-configures the cluster to use from your K8S config file, this is assumed to be `~/.kube/config`
unless the `KUBECONFIG` environment variable is set to override this location.  By default this will use whatever your 
current context is in the config file, to use an alternative context from your config file you can specify the 
`--context <context>` flag with the desired context.

You can optionally use a different K8S master URL than the one your K8S config file specified, this should be supplied 
via the `--spark-master <master-url>` flag.

## Re-using Docker Images

By default, the test framework will build new Docker images on every test execution. A unique image tag is generated,
and it is written to file at `target/imageTag.txt`. To reuse the images built in a previous run, or to use a Docker 
image tag that you have built by other means already, pass the tag to the test script:

    dev/dev-run-integration-tests.sh --image-tag <tag>

where if you still want to use images that were built before by the test framework:

    dev/dev-run-integration-tests.sh --image-tag $(cat target/imageTag.txt)
    
### Customising the Image Names

If your image names do not follow the standard Spark naming convention - `spark`, `spark-py` and `spark-r` - then you can customise the names using several options.

If you use the same basic pattern but a different prefix for the name e.g. `apache-spark` you can just set `--base-image-name <base-name>` e.g.

    dev/dev-run-integration-tests.sh --base-image-name apache-spark
    
Alternatively if you use completely custom names then you can set each individually via the `--jvm-image-name <name>`, `--python-image-name <name>` and `--r-image-name <name>` arguments e.g.

    dev/dev-run-integration-tests.sh --jvm-image-name jvm-spark --python-image-name pyspark --r-image-name sparkr

## Spark Distribution Under Test

The Spark code to test is handed to the integration test system via a tarball. Here is the option that is used to 
specify the tarball:

* `--spark-tgz <path-to-tgz>` - set `<path-to-tgz>` to point to a tarball containing the Spark distribution to test.

This Tarball should be created by first running `dev/make-distribution.sh` passing the `--tgz` flag and `-Pkubernetes` 
as one of the options to ensure that Kubernetes support is included in the distribution.  For more details on building a
runnable distribution please see the 
[Building Spark](https://spark.apache.org/docs/latest/building-spark.html#building-a-runnable-distribution) 
documentation.

**TODO:** Don't require the packaging of the built Spark artifacts into this tarball, just read them out of the current 
tree.

## Customizing the Namespace and Service Account

If no namespace is specified then a temporary namespace will be created and deleted during the test run.  Similarly if 
no service account is specified then the `default` service account for the namespace will be used.

Using the `--namespace <namespace>` flag sets `<namespace>` to the namespace in which the tests should be run.  If this 
is supplied then the tests assume this namespace exists in the K8S cluster and will not attempt to create it.  
Additionally this namespace must have an appropriately authorized service account which can be customised via the 
`--service-account` flag.

The `--service-account <service account name>` flag sets `<service account name>` to the name of the Kubernetes service 
account to use in the namespace specified by the `--namespace` flag. The service account is expected to have permissions
to get, list, watch, and create pods. For clusters with RBAC turned on, it's important that the right permissions are 
granted to the service account in the namespace through an appropriate role and role binding. A reference RBAC 
configuration is provided in `dev/spark-rbac.yaml`.

# Running the Test Directly

If you prefer to run just the integration tests directly, then you can customise the behaviour via passing system 
properties to Maven.  For example:

    mvn integration-test -am -pl :spark-kubernetes-integration-tests_2.12 \
                            -Pkubernetes -Pkubernetes-integration-tests \ 
                            -Phadoop-2.7 -Dhadoop.version=2.7.4 \
                            -Dspark.kubernetes.test.sparkTgz=spark-3.0.0-SNAPSHOT-bin-example.tgz \
                            -Dspark.kubernetes.test.imageTag=sometag \
                            -Dspark.kubernetes.test.imageRepo=docker.io/somerepo \
                            -Dspark.kubernetes.test.namespace=spark-int-tests \
                            -Dspark.kubernetes.test.deployMode=docker-for-desktop \
                            -Dtest.include.tags=k8s
                            
                            
## Available Maven Properties

The following are the available Maven properties that can be passed.  For the most part these correspond to flags passed 
to the wrapper scripts and using the wrapper scripts will simply set these appropriately behind the scenes.

<table>
  <tr>
    <th>Property</th>
    <th>Description</th>
    <th>Default</th>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.sparkTgz</code></td>
    <td>
      A runnable Spark distribution to test.
    </td>
    <td></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.unpackSparkDir</code></td>
    <td>
      The directory where the runnable Spark distribution will be unpacked.
    </td>
    <td><code>${project.build.directory}/spark-dist-unpacked</code></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.deployMode</code></td>
    <td>
      The integration test backend to use.  Acceptable values are <code>minikube</code>, 
      <code>docker-for-desktop</code> and <code>cloud</code>.
    <td><code>minikube</code></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.kubeConfigContext</code></td>
    <td>
      When using the <code>cloud</code> backend specifies the context from the users K8S config file that should be used
      as the target cluster for integration testing.  If not set and using the <code>cloud</code> backend then your 
      current context will be used.
    </td>
    <td></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.master</code></td>
    <td>
      When using the <code>cloud-url</code> backend must be specified to indicate the K8S master URL to communicate 
      with.
    </td>
    <td></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.imageTag</code></td>
    <td>
      A specific image tag to use, when set assumes images with those tags are already built and available in the 
      specified image repository.  When set to <code>N/A</code> (the default) fresh images will be built.
    </td>
    <td><code>N/A</code>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.imageTagFile</code></td>
    <td>
      A file containing the image tag to use, if no specific image tag is set then fresh images will be built with a 
      generated tag and that tag written to this file.
    </td>
    <td><code>${project.build.directory}/imageTag.txt</code></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.imageRepo</code></td>
    <td>
      The Docker image repository that contains the images to be used if a specific image tag is set or to which the 
      images will be pushed to if fresh images are being built.
    </td>
    <td><code>docker.io/kubespark</code></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.jvmImage</code></td>
    <td>
      The image name for the JVM based Spark image to test
    </td>
    <td><code>spark</code></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.pythonImage</code></td>
    <td>
      The image name for the Python based Spark image to test
    </td>
    <td><code>spark-py</code></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.rImage</code></td>
    <td>
      The image name for the R based Spark image to test
    </td>
    <td><code>spark-r</code></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.namespace</code></td>
    <td>
      A specific Kubernetes namespace to run the tests in.  If specified then the tests assume that this namespace 
      already exists. When not specified a temporary namespace for the tests will be created and deleted as part of the
      test run.
    </td>
    <td></td>
  </tr>
  <tr>
    <td><code>spark.kubernetes.test.serviceAccountName</code></td>
    <td>
      A specific Kubernetes service account to use for running the tests.  If not specified then the namespaces default
      service account will be used and that must have sufficient permissions or the tests will fail.
    </td>
    <td></td>
  </tr>
</table>
