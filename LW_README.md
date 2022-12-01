# Apache Spark for Lacework

Main reasons for lacework to fork Apache spark source code - 
1. Spark current version is building source code with deprecated `openjdk:11-jre-slim`( refer this URL for notice - https://hub.docker.com/_/openjdk), this means build is older, missing any security patches or bug fixes. 
2. Lacework batch/service code is built using Amazon Corretto Java 8 but has problem running on `openjdk:11-jre-slim`.

## Build instructions
Apache spark is built using Maven and following are the steps.
```bash
export MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g"

## Build source code
./build/mvn -DskipTests clean package
```

Build the distributable package with Kubernetes support
```bazaar
./dev/make-distribution.sh --name custom-spark --pip --r --tgz -Psparkr -Phive -Phive-thriftserver -Pmesos -Pyarn -Pkubernetes
./build/mvn -Pkubernetes -DskipTests clean package
```

Follow this document https://spark.apache.org/docs/latest/building-spark.html
