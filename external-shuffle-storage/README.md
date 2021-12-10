# External Shuffle Storage

This module provides support to store shuffle files on external shuffle storage like S3. It helps Dynamic
Allocation on Kubernetes. Spark driver could release idle executors without worrying about losing
shuffle data because the shuffle data is store on external shuffle storage which are different 
from executors.

This module implements a new Shuffle Manager named as StarShuffleManager, and copies a lot of codes
from Spark SortShuffleManager. This is for a quick prototype. We want to use this as an example to discuss
with Spark community and get feedback. We will work with the community to remove code duplication later
and make StarShuffleManager more integrated with Spark code.

## How to Build Spark Distribution with StarShuffleManager jar File

Follow [Building Spark](https://spark.apache.org/docs/latest/building-spark.html) instructions,
with extra `-Pexternal-shuffle-storage` to generate the new shuffle implementation jar file.

Following is one command example to use `dev/make-distribution.sh` under Spark repo root directory:

```
./dev/make-distribution.sh --name spark-with-external-shuffle-storage --pip --tgz -Phive -Phive-thriftserver -Pkubernetes -Phadoop-3.2 -Phadoop-cloud -Dhadoop.version=3.2.0 -Pexternal-shuffle-storage
```

If you want to build a Spark docker image, you could unzip the Spark distribution tgz file, and run command like following:

```
./bin/docker-image-tool.sh -t spark-with-external-shuffle-storage build
```

This command creates `external-shuffle-storage_xxx.jar` file for StarShuffleManager
under `jars` directory in the generated Spark distribution. Now you could use this Spark 
distribution to run your Spark application with external shuffle storage.

## How to Run Spark Application With External Shuffle Storage in Kubernetes

### Run Spark Application With S3 as External Shuffle Storage and Dynamic Allocation

Add configure to your Spark application like following (you need to adjust the values based on your environment):

```
spark.shuffle.manager=org.apache.spark.shuffle.StarShuffleManager
spark.shuffle.star.rootDir=s3://my_bucket_name/my_shuffle_folder
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.shuffleTracking.enabled=true
spark.dynamicAllocation.shuffleTracking.timeout=1
```
