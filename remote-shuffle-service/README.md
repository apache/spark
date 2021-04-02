# Remote Shuffle Service

Remote Shuffle Service stores shuffle files on dedicated shuffle servers. It helps Dynamic
Allocation on Kubernetes. Spark driver could release idle executors without worrying about losing
shuffle data because the shuffle data is store on dedicated shuffle servers which are different 
from executors.

The high level design for Remote Shuffle Service could be found [here](https://github.com/uber/RemoteShuffleService/blob/master/docs/server-high-level-design.md).

## How to Build Server/Client

Make sure JDK and maven are installed on your machine.

### Build Server Side Jar File

- Run following command inside this project directory: 

```
mvn clean package -DskipTests -Premote-shuffle-service-server
```

This command creates `remote-shuffle-service-server.jar` file under `target` directory.

### Build Spark Distribution with Client Side Jar File

Follow [Building Spark](https://spark.apache.org/docs/latest/building-spark.html) instructions,
with extra `-Premote-shuffle-service` to generate remote shuffle service client side jar file.

Following is one command example to use `dev/make-distribution.sh` under Spark repo root directory:

```
./dev/make-distribution.sh --name spark-with-remote-shuffle-service-client --pip --tgz -Phive -Phive-thriftserver -Pkubernetes -Phadoop-3.2 -Phadoop-cloud -Dhadoop.version=3.2.2 -Premote-shuffle-service
```

This command creates `remote-shuffle-service_xxx.jar` file for remote shuffle service client 
under `jars` directory in the generated Spark distribution. Now you could use this Spark 
distribution to run your Spark application with remote shuffle service.

## How to Run Spark Application With Remote Shuffle Service in Kubernetes

### Build Server Side Docker Image

```
docker build -t remote-shuffle-service-server:0.0.1 .
```

### Deploy As StatefulSet In Kubernetes Cluster

Modify `kubernetes.yml` under this project, replace `image: [remote-shuffle-service-server-image]`
with your own docker image name. Then run following:

```
kubectl apply -f kubernetes.yml
```

### Run Spark Application With Remote Shuffle Service Client and Dynamic Allocation

Add configure to your Spark application like following (you need to adjust the values based on your environment):

```
spark.shuffle.manager=org.apache.spark.shuffle.RssShuffleManager
spark.shuffle.rss.serviceRegistry.type=serverSequence
spark.shuffle.rss.serverSequence.connectionString=rss-%s.rss.remote-shuffle-service.svc.cluster.local:9338
spark.shuffle.rss.serverSequence.startIndex=0
spark.shuffle.rss.serverSequence.endIndex=1
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.shuffleTracking.enabled=true
spark.dynamicAllocation.shuffleTracking.timeout=1
```


