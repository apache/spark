# Spark Connect

This module contains the implementation of Spark Connect which is a logical plan
facade for the implementation in Spark. Spark Connect is directly integrated into the build
of Spark.

The documentation linked here is specifically for developers of Spark Connect and not
directly intended to be end-user documentation.

## Development Topics

### Guidelines for new clients

When contributing a new client please be aware that we strive to have a common
user experience across all languages. Please follow the below guidelines:

* [Connection string configuration](docs/client-connection-string.md)
* [Adding new messages](docs/adding-proto-messages.md) in the Spark Connect protocol.

### Python client development

Python-specific development guidelines are located in [python/docs/source/development/testing.rst](https://github.com/apache/spark/blob/master/python/docs/source/development/testing.rst) that is published at [Development tab](https://spark.apache.org/docs/latest/api/python/development/index.html) in PySpark documentation.

To generate the Python client code from the proto files:

First, make sure to have a Python environment with the installed dependencies.
Specifically, install `black` and dependencies from the "Spark Connect python proto generation plugin (optional)" section.


```
pip install -r dev/requirements.txt
```

Install [buf](https://github.com/bufbuild/buf)

```
brew install bufbuild/buf/buf
```

Generate the Python files by running:

```
dev/connect-gen-protos.sh
```

### Build with user-defined `protoc` and `protoc-gen-grpc-java`

When the user cannot use the official `protoc` and `protoc-gen-grpc-java` binary files to build the `connect` module in the compilation environment,
for example, compiling `connect` module on CentOS 6 or CentOS 7 which the default `glibc` version is less than 2.14, we can try to compile and test by 
specifying the user-defined `protoc` and `protoc-gen-grpc-java` binary files as follows:

```bash
export SPARK_PROTOC_EXEC_PATH=/path-to-protoc-exe
export CONNECT_PLUGIN_EXEC_PATH=/path-to-protoc-gen-grpc-java-exe
./build/mvn -Phive -Puser-defined-protoc clean package
```

or

```bash
export SPARK_PROTOC_EXEC_PATH=/path-to-protoc-exe
export CONNECT_PLUGIN_EXEC_PATH=/path-to-protoc-gen-grpc-java-exe
./build/sbt -Puser-defined-protoc clean package
```

The user-defined `protoc` and `protoc-gen-grpc-java` binary files can be produced in the user's compilation environment by source code compilation, 
for compilation steps, please refer to [protobuf](https://github.com/protocolbuffers/protobuf) and [grpc-java](https://github.com/grpc/grpc-java).

