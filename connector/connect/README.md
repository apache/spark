# Spark Connect - Developer Documentation

**Spark Connect is a strictly experimental feature and under heavy development.
All APIs should be considered volatile and should not be used in production.**

This module contains the implementation of Spark Connect which is a logical plan
facade for the implementation in Spark. Spark Connect is directly integrated into the build
of Spark. To enable it, you only need to activate the driver plugin for Spark Connect.

The documentation linked here is specifically for developers of Spark Connect and not
directly intended to be end-user documentation.


## Getting Started 

### Build

```bash
./build/mvn -Phive clean package
```

or

```bash
./build/sbt -Phive clean package
```

### Build with user-defined `protoc` and `protoc-gen-grpc-java`

When the user cannot use the official `protoc` and `protoc-gen-grpc-java` binary files to build the `connect` module in the compilation environment,
for example, compiling `connect` module on CentOS 6 or CentOS 7 which the default `glibc` version is less than 2.14, we can try to compile and test by 
specifying the user-defined `protoc` and `protoc-gen-grpc-java` binary files as follows:

```bash
export CONNECT_PROTOC_EXEC_PATH=/path-to-protoc-exe
export CONNECT_PLUGIN_EXEC_PATH=/path-to-protoc-gen-grpc-java-exe
./build/mvn -Phive -Puser-defined-protoc clean package
```

or

```bash
export CONNECT_PROTOC_EXEC_PATH=/path-to-protoc-exe
export CONNECT_PLUGIN_EXEC_PATH=/path-to-protoc-gen-grpc-java-exe
./build/sbt -Puser-defined-protoc clean package
```

The user-defined `protoc` and `protoc-gen-grpc-java` binary files can be produced in the user's compilation environment by source code compilation, 
for compilation steps, please refer to [protobuf](https://github.com/protocolbuffers/protobuf) and [grpc-java](https://github.com/grpc/grpc-java).


### Run Spark Shell

To run Spark Connect you locally built:

```bash
# Scala shell
./bin/spark-shell \
  --jars `ls connector/connect/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
  --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin

# PySpark shell
./bin/pyspark \
  --jars `ls connector/connect/target/**/spark-connect*SNAPSHOT.jar | paste -sd ',' -` \
  --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
```

To use the release version of Spark Connect:

```bash
./bin/spark-shell \
  --packages org.apache.spark:spark-connect_2.12:3.4.0 \
  --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
```

### Run Tests

```bash
# Run a single Python class.
./python/run-tests --testnames 'pyspark.sql.tests.connect.test_connect_basic'
```

```bash
# Run all Spark Connect Python tests as a module.
./python/run-tests --module pyspark-connect --parallelism 1
```


## Development Topics

### Generate proto generated files for the Python client
1. Install `buf version 1.9.0`: https://docs.buf.build/installation
2. Run `pip install grpcio==1.48.1 protobuf==3.19.5 mypy-protobuf==3.3.0`
3. Run `./connector/connect/dev/generate_protos.sh`
4. Optional Check `./dev/check-codegen-python.py`

### Guidelines for new clients

When contributing a new client please be aware that we strive to have a common
user experience across all languages. Please follow the below guidelines:

* [Connection string configuration](docs/client-connection-string.md)
* [Adding new messages](docs/adding-proto-messages.md) in the Spark Connect protocol.
