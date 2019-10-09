# spark-operator

[![Build status](https://travis-ci.org/radanalyticsio/spark-operator.svg?branch=master)](https://travis-ci.org/radanalyticsio/spark-operator)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

`{CRD|ConfigMap}`-based approach for managing the Spark clusters in Kubernetes and OpenShift.

This operator uses [abstract-operator](https://github.com/jvm-operators/abstract-operator) library.

<!--
asciinema rec -i 3
docker run -\-rm -v $PWD:/data asciinema/asciicast2gif -s 1.18 -w 104 -h 27 -t monokai 189204.cast demo.gif
-->
[![Watch the full asciicast](https://github.com/radanalyticsio/spark-operator/raw/master/docs/ascii.gif)](https://asciinema.org/a/230927?&cols=123&rows=27&theme=monokai)

# How does it work
![UML diagram](https://github.com/radanalyticsio/spark-operator/raw/master/docs/standardized-UML-diagram.png "UML Diagram")

# Quick Start

Run the `spark-operator` deployment:
```bash
kubectl apply -f manifest/operator.yaml
```

Create new cluster from the prepared example:

```bash
kubectl apply -f examples/cluster.yaml
```

After issuing the commands above, you should be able to see a new Spark cluster running in the current namespace.

```bash
kubectl get pods
NAME                               READY     STATUS    RESTARTS   AGE
my-spark-cluster-m-5kjtj           1/1       Running   0          10s
my-spark-cluster-w-m8knz           1/1       Running   0          10s
my-spark-cluster-w-vg9k2           1/1       Running   0          10s
spark-operator-510388731-852b2     1/1       Running   0          27s
```

Once you don't need the cluster anymore, you can delete it by deleting the custom resource by:
```bash
kubectl delete sparkcluster my-spark-cluster
```

# Very Quick Start

```bash
# create operator
kubectl apply -f http://bit.ly/sparkop

# create cluster
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: SparkCluster
metadata:
  name: my-cluster
spec:
  worker:
    instances: "2"
EOF
```

## Spark Applications

Apart from managing clusters with Apache Spark, this operator can also manage Spark applications similarly as the `GoogleCloudPlatform/spark-on-k8s-operator`. These applications spawn their own Spark cluster for their needs and it uses the Kubernetes as the native scheduling mechanism for Spark. For more details, consult the [Spark docs](https://spark.apache.org/docs/latest/running-on-kubernetes.html).

```bash
# create spark application
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: SparkApplication
metadata:
  name: my-cluster
spec:
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar
  mainClass: org.apache.spark.examples.SparkPi
EOF
```

### OpenShift

For deployment on OpenShift use the same commands as above (with `oc` instead of `kubectl` if `kubectl` is not installed) and make sure the logged user can create CRDs: `oc login -u system:admin && oc project default`

### Config Map approach

This operator can also work with Config Maps instead of CRDs. This can be useful in situations when user is not allowed to create CRDs or `ClusterRoleBinding` resources. The schema for config maps is almost identical to custom resources and you can check the [examples](./examples/test/cm).

```bash
kubectl apply -f manifest/operator-cm.yaml
```

The manifest above is almost the same as the [operator.yaml](./manifest/operator.yaml). If the environmental variable `CRD` is set to `false`, the operator will watch on config maps with certain labels.

You can then create the Spark clusters as usual by creating the config map (CM).

```bash
kubectl apply -f examples/cluster-cm.yaml
kubectl get cm -l radanalytics.io/kind=SparkCluster
```

or Spark applications that are natively scheduled on Spark clusters by:

```bash
kubectl apply -f examples/test/cm/app.yaml
kubectl get cm -l radanalytics.io/kind=SparkApplication
```

### Images

Image name         | Description | Layers | quay.io | docker.io
------------------ | ----------- | ------ | ------- | ----------
`:latest-released` | represents the latest released version | [![Layers info](https://images.microbadger.com/badges/image/radanalyticsio/spark-operator:latest-released.svg)](https://microbadger.com/images/radanalyticsio/spark-operator:latest-released) | [![quay.io repo](https://quay.io/repository/radanalyticsio/spark-operator/status "quay.io repo")](https://quay.io/repository/radanalyticsio/spark-operator?tab=tags) | [![docker.io repo](https://img.shields.io/docker/pulls/radanalyticsio/spark-operator.svg "docker.io repo")](https://hub.docker.com/r/radanalyticsio/spark-operator/tags/)
`:latest`          | represents the master branch | [![Layers info](https://images.microbadger.com/badges/image/radanalyticsio/spark-operator:latest.svg)](https://microbadger.com/images/radanalyticsio/spark-operator:latest) |  | 
`:x.y.z`           | one particular released version | [![Layers info](https://images.microbadger.com/badges/image/radanalyticsio/spark-operator:0.1.5.svg)](https://microbadger.com/images/radanalyticsio/spark-operator:0.1.5) |  | 

For each variant there is also available an image with `-alpine` suffix based on Alpine for instance [![Layers info](https://images.microbadger.com/badges/image/radanalyticsio/spark-operator:latest-released-alpine.svg)](https://microbadger.com/images/radanalyticsio/spark-operator:latest-released-alpine)

### Configuring the operator

The spark-operator contains several defaults that are implicit to the creation
of Spark clusters and applications. Here are a list of environment variables
that can be set to adjust the default behaviors of the operator.

* `CRD` set to `true` if the operator should respond to Custom
  Resources, and set to `false` if it should respond to ConfigMaps.
* `DEFAULT_SPARK_CLUSTER_IMAGE` a container image reference that will be used
  as a default for all pods in a `SparkCluster` deployment when the image is
  not specified in the cluster manifest.
* `DEFAULT_SPARK_APP_IMAGE` a container image reference that will be used as a
  default for all executor pods in a `SparkApplication` deployment when the
  image is not specified in the application manifest.

_Please note that these environment variables must be set in the operator's
container, see [operator.yaml](manifest/operator.yaml) and
[operator-cm.yaml](manifest/operator-cm.yaml) for operator deployment information._

### Related projects

If you are looking for tooling to make interacting with the spark-operator
more convenient, please see the following.

* [Ansible role](https://github.com/jvm-operators/ansible-openshift-spark-operator) is a simple way to
deploy the Spark operator using Ansible ecosystem. The role is [available](https://galaxy.ansible.com/jiri_kremser/spark_operator) also in the Ansible Galaxy.

* [oshinko-temaki](https://pypi.org/project/oshinko-temaki/) is a shell
  application for generating `SparkCluster` manifest definitions. It can
  produce full schema manifests from a few simple command line flags.

For checking and verifying that your own container image will work smoothly with the operator
use the following tool.

* [soit](https://pypi.org/project/soit/) is a CLI tool that runs a set of tests against the 
given image to verify if it contains the right files on the file system, 
if worker can register with master, etc. Check the code in the 
[repository](https://github.com/Jiri-Kremser/spark-operator-image-tool).

The radanalyticsio/spark-operator is not the only Kubernetes operator service
that targets Apache Spark.

* [GoogleCloudPlatform/spark-on-k8s-operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
  is an operator which shares a similar schema for the Spark cluster and application
  resources. One major difference between it and the `radanalyticsio/spark-operator`
  is that the latter has been designed to work well in environments where a
  user has a limited role-based access to Kubernetes, such as on OpenShift and also that
  `radanalyticsio/spark-operator` can deploy standalone Spark clusters.

### Operator Marketplace

If you would like to install the operator into OpenShift (since 4.1) using the [Operator Marketplace](https://github.com/operator-framework/operator-marketplace), simply run:

```bash
cat <<EOF | kubectl apply -f -
apiVersion: operators.coreos.com/v1
kind: OperatorSource
metadata:
  name: radanalyticsio-operators
  namespace: openshift-marketplace
spec:
  type: appregistry
  endpoint: https://quay.io/cnr
  registryNamespace: radanalyticsio
  displayName: "Operators from radanalytics.io"
  publisher: "Jirka Kremser"
EOF
```

You will find the operator in the OpenShift web console under `Catalog > OperatorHub` (make sure the namespace is set to `openshift-marketplace`).

### Troubleshooting

Show the log:

```bash
# last 25 log entries
kubectl logs --tail 25 -l app.kubernetes.io/name=spark-operator
```

```bash
# follow logs
kubectl logs -f `kubectl get pod -l app.kubernetes.io/name=spark-operator -o='jsonpath="{.items[0].metadata.name}"' | sed 's/"//g'`
```

Run the operator from your host (also possible with the debugger/profiler):

```bash
java -jar target/spark-operator-*.jar
```
