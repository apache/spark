# Using shared volume

Assumptions:
* Currently it's openshift only
* Make sure the `/tmp/spark-events` on host is writable and readable by 'others', or use different directory in the PVs.
* Spark 2.4 is installed locally and `spark-submit` is on `$PATH`
* Spark operator is up and running

```
oc apply -f examples/test/history-server/sharedVolume/
```

```
oc get route
```

Open `http://my-history-server-default.127.0.0.1.nip.io/` or similar url in browser.

```
oc get pods -lradanalytics.io/podType=master -owide
```

Instead of `172.17.0.2`, use the correct ip from the command above
```
_jar_path=`type -P spark-submit | xargs dirname`/../examples/jars/spark-examples_*.jar
spark-submit --master spark://172.17.0.2:7077 \
 --conf spark.eventLog.enabled=true \
 --conf spark.eventLog.dir=/tmp/spark-events/ \
 --class org.apache.spark.examples.SparkPi \
 --executor-memory 1G \
 $_jar_path 42
```


# Using external object storage

Assumptions:
* Openshift is up and running (currently it's openshift only)
* `aws` client is installed and configured on `$PATH`
* Spark 2.4 is installed locally and `spark-submit` is on `$PATH`


Deploy the operator (and wait for it to start):

```
oc login -u system:admin ; oc project default ; oc apply -f manifest/operator.yaml
```

Deploy ceph-nano, Minio or use S3 from Amazon directly:

```
oc --as system:admin adm policy add-scc-to-user anyuid system:serviceaccount:default:default
oc apply -f examples/test/history-server/externalStorage/
```

Configure the aws client:

```
aws configure
AWS Access Key ID = foo
AWS Secret Access Key = bar
```

Create new emtpy bucket for the event log called `my-history-server`:

```
oc expose pod ceph-nano-0 --type=NodePort
_ceph=http://`oc get svc ceph-nano-0 --template={{.spec.clusterIP}}`:8000
aws s3api create-bucket --bucket my-history-server --endpoint-url=$_ceph
```

Create some dummy file in the bucket (sparks needs the bucket to be non-empty from some reason):

```
aws s3 cp README.md s3://my-history-server/ --endpoint-url=$_ceph
```


```
oc get pods -lradanalytics.io/podType=master -owide
```

Instead of `172.17.0.2`, use the correct ip from the command above

```
_jar_path=`type -P spark-submit | xargs dirname`/../examples/jars/spark-examples_*.jar
spark-submit --master spark://172.17.0.4:7077 \
 --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.3 \
 --conf spark.eventLog.enabled=true \
 --conf spark.eventLog.dir=s3a://my-history-server/ \
 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
 --conf spark.hadoop.fs.s3a.access.key=foo \
 --conf spark.hadoop.fs.s3a.secret.key=bar \
 --conf spark.hadoop.fs.s3a.endpoint=$_ceph \
 --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
 --class org.apache.spark.examples.SparkPi \
 --executor-memory 1G \
 $_jar_path 42
 ```
 
 
 
 Delete the dummy file in the bucket (todo ugly):
 
 ```
 aws s3 rm s3://my-history-server/README.md --endpoint-url=$_ceph
 ```

 Check if the event has been written to the bucket:

 ```
 aws s3 ls s3://my-history-server/ --endpoint-url=$_ceph
 ```

Check the history server on:

```
oc get route
```

Open `http://my-history-server-default.127.0.0.1.nip.io/` or similar url in browser.