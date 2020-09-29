---
layout: global
title: MLlib Linear Algebra Acceleration Guide
displayTitle: MLlib Linear Algebra Acceleration Guide
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

## Introduction

This guide provides necessary information to enable accelerated linear algebra processing for Spark MLlib.

Spark MLlib defines Vector and Matrix as basic data types for machine learning algorithms. On top of them, [BLAS](https://en.wikipedia.org/wiki/Basic_Linear_Algebra_Subprograms) and [LAPACK](https://en.wikipedia.org/wiki/LAPACK) operations are implemented and supported by [netlib-java](https://github.com/fommil/netlib-Java) (the algorithms may call [Breeze](https://github.com/scalanlp/breeze) and it will in turn call `netlib-java`). `netlib-java` can use optimized native linear algebra libraries (refered to as "native libraries" or "BLAS libraries" hereafter) for faster numerical processing. [Intel MKL](https://software.intel.com/content/www/us/en/develop/tools/math-kernel-library.html) and [OpenBLAS](http://www.openblas.net) are two popular ones.

However due to license differences, the official released Spark binaries by default don't contain native libraries support for `netlib-java`.

The following sections describe how to enable `netlib-java` with native libraries support for Spark MLlib and how to install native libraries and configure them properly.

## Enable `netlib-java` with native library proxies 

`netlib-java` depends on `libgfortran`. It requires GFORTRAN 1.4 or above. This can be obtained by installing `libgfortran` package. After installation, the following command can be used to verify if it is installed properly.
```
strings /path/to/libgfortran.so.3.0.0 | grep GFORTRAN_1.4
```

To build Spark with `netlib-java` native library proxies, you need to add `-Pnetlib-lgpl` to Maven build command line. For example:
```
$SPARK_SOURCE_HOME/build/mvn -Pnetlib-lgpl -DskipTests -Pyarn -Phadoop-2.7 clean package
```

If you only want to enable it in your project, include `com.github.fommil.netlib:all:1.1.2` as a dependency of your project.

## Install native linear algebra libraries

Intel MKL and OpenBLAS are two popular native linear algebra libraries. You can choose one of them based on your preference. We provide basic instructions as below. You can refer to [netlib-java documentation](https://github.com/fommil/netlib-java) for more advanced installation instructions.

### Intel MKL

- Download and install Intel MKL. The installation should be done on all nodes of the cluster. We assume the installation location is $MKLROOT (e.g. /opt/intel/mkl).
- Create soft links to `libmkl_rt.so` with specific names in system library search paths. For instance, make sure `/usr/local/lib` is in system library search paths and run the following commands:
```
$ ln -sf $MKLROOT/lib/intel64/libmkl_rt.so /usr/local/lib/libblas.so.3
$ ln -sf $MKLROOT/lib/intel64/libmkl_rt.so /usr/local/lib/liblapack.so.3
```

### OpenBLAS

The installation should be done on all nodes of the cluster. Generic version of OpenBLAS are available with most distributions. You can install it with a distribution package manager like `apt` or `yum`.

For Debian / Ubuntu:
```
sudo apt-get install libopenblas-base
sudo update-alternatives --config libblas.so.3
```
For CentOS / RHEL:
```
sudo yum install openblas
```

## Check if native libraries are enabled for MLlib

To verify native libraries are properly loaded, start `spark-shell` and run the following code:
```
scala> import com.github.fommil.netlib.BLAS;
scala> System.out.println(BLAS.getInstance().getClass().getName());
```

If they are correctly loaded, it should print `com.github.fommil.netlib.NativeSystemBLAS`. Otherwise the warnings should be printed:
```
WARN BLAS: Failed to load implementation from:com.github.fommil.netlib.NativeSystemBLAS
WARN BLAS: Failed to load implementation from:com.github.fommil.netlib.NativeRefBLAS
```

If native libraries are not properly configured in the system, the Java implementation (f2jBLAS) will be used as fallback option.

## Spark Configuration

The default behavior of multi-threading in either Intel MKL or OpenBLAS may not be optimal with Spark's execution model [^1].

Therefore configuring these native libraries to use a single thread for operations may actually improve performance (see [SPARK-21305](https://issues.apache.org/jira/browse/SPARK-21305)). It is usually optimal to match this to the number of `spark.task.cpus`, which is `1` by default and typically left at `1`.

You can use the options in `config/spark-env.sh` to set thread number for Intel MKL or OpenBLAS:
* For Intel MKL:
```
MKL_NUM_THREADS=1
```
* For OpenBLAS:
```
OPENBLAS_NUM_THREADS=1
```

[^1]: Please refer to the following resources to understand how to configure the number of threads for these BLAS implementations: [Intel MKL](https://software.intel.com/en-us/articles/recommended-settings-for-calling-intel-mkl-routines-from-multi-threaded-applications) or [Intel oneMKL](https://software.intel.com/en-us/onemkl-linux-developer-guide-improving-performance-with-threading) and [OpenBLAS](https://github.com/xianyi/OpenBLAS/wiki/faq#multi-threaded).
