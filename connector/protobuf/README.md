# Spark Protobuf - Developer Documentation

## Getting Started 

### Build

```bash
./build/mvn clean package
```

or

```bash
./build/sbt clean package
```

### Build with user-defined `protoc`

When the user cannot use the official `protoc` binary files to build the `protobuf` module in the compilation environment,
for example, compiling `protobuf` module on CentOS 6 or CentOS 7 which the default `glibc` version is less than 2.14, we can try to compile and test by 
specifying the user-defined `protoc` binary files as follows:

```bash
export SPARK_PROTOC_EXEC_PATH=/path-to-protoc-exe
./build/mvn -Phive -Puser-defined-protoc clean package
```

or

```bash
export SPARK_PROTOC_EXEC_PATH=/path-to-protoc-exe
./build/sbt -Puser-defined-protoc clean package
```

The user-defined `protoc` binary files can be produced in the user's compilation environment by source code compilation, 
for compilation steps, please refer to [protobuf](https://github.com/protocolbuffers/protobuf).
