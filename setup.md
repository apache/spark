# Spark Build Setup Guide

This document describes how to build Apache Spark (v4.0.1) from source using Docker.
The build requires ~10–12 GB of free disk space, mostly for Maven dependencies and compiled artifacts.

## Build Container

```bash
docker build -t sparkdev .
```

## Launch Container
```bash
docker run -it --rm -v $(pwd):/workspace sparkdev
```

## Build Spark
This only needs to be done once. Inside the docker container run :
```bash
./build/mvn -DskipTests -T1C clean package
```

