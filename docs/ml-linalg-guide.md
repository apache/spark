# Spark MLlib Linear Algebra Acceleration Guide

## Introduction

This guide provides necessary information to enable accelerated linear algebra processing for Spark MLlib.

Spark MLlib defined Vector and Matrix basic data types for machine learning algorithms. On top of them, [BLAS](https://en.wikipedia.org/wiki/Basic_Linear_Algebra_Subprograms) and [LAPACK](https://en.wikipedia.org/wiki/LAPACK) operations are implemented and supported by [netlib-java](https://github.com/fommil/netlib-Java).[^1] `netlib-java` can use optimized native libraries for faster numerical processing. [Intel MKL](https://software.intel.com/content/www/us/en/develop/tools/math-kernel-library.html) and [OpenBLAS](http://www.openblas.net) are two most popular native linear algebra libraries (refered to as "native libraries" hereafter).

However due to license restrictions, the official released Spark binaries by default doesn't contain native libraries enabled for `netlib-java`.

The following sections describe how to enable `netlib-java` with native libraries support, to install the native libraries and configure them properly.

[^1]: The algorithms may call Breeze and it will in turn call `netlib-java`.

## Enable `netlib-java`

To build Spark with `netlib-java` native library proxies, you need to add `-Pnetlib-lgpl` profile to Maven build command line. For example:
```
$SPARK_SOURCE_HOME/build/mvn -Pnetlib-lgpl -DskipTests -Pyarn -Phadoop-2.7 clean package
```

If you only want to enable it in your project, include `com.github.fommil.netlib:all:1.1.2` as a dependency of your project.


and read the [netlib-java](https://github.com/fommil/netlib-java) documentation for your platform's additional installation instructions.

## Install Native System Libraries

Intel MKL and OpenBLAS are two most popular native linear algebra libraries, you can choose one of them based on your preference. We will describe how to install them for `netlib-java`.

### Intel MKL

1. Download and install Intel MKL. The installation should be done on all nodes of the cluster. We assume the installation location is $MKLROOT.
2. Make sure `/usr/local/lib` is in system library search path and run the following commands:
```
$ ln -sf $MKLROOT/lib/intel64/libmkl_rt.so /usr/local/lib/libblas.so.3
$ ln -sf $MKLROOT/lib/intel64/libmkl_rt.so /usr/local/lib/liblapack.so.3
```

### OpenBLAS

TBD

## Check if native libraries are enabled for MLlib

To verify native libraries are properly loaded, start `spark-shell` and run the following code
```
scala> import com.github.fommil.netlib.BLAS;
scala> System.out.println(BLAS.getInstance().getClass().getName());
```

If they arecorrectly loaded, it should print `com.github.fommil.netlib.NativeSystemBLAS`. Otherwise the warnings should be printed:
```
WARN BLAS: Failed to load implementation from:com.github.fommil.netlib.NativeSystemBLAS
WARN BLAS: Failed to load implementation from:com.github.fommil.netlib.NativeRefBLAS
```

if native libraries are not properly configured in the system, Java implementation(f2jBLAS) will be used as fallback option.

## Spark Configuration

The use of multiple-threading in either Intel MKL or OpenBLAS can conflict with Spark's execution model.[^2]

Therefore configuring these native libraries to use a single thread for operations may actually improve performance (see [SPARK-21305](https://issues.apache.org/jira/browse/SPARK-21305)). It is usually optimal to match this to the number of `spark.task.cpus`, which is 1 by default and typically left at 1.

Use the options in `config/spark-env.sh` to disable multi-threading by setting thread number to 1.
```
# You might get better performance to enable these options if using native BLAS (see SPARK-21305).
# - MKL_NUM_THREADS=1        Disable multi-threading of Intel MKL
# - OPENBLAS_NUM_THREADS=1   Disable multi-threading of OpenBLAS
```

[^2]: Please refer to resources like the following to understand how to configure the number of threads these BLAS implementations use: [Intel MKL](https://software.intel.com/en-us/articles/recommended-settings-for-calling-intel-mkl-routines-from-multi-threaded-applications) or [Intel oneMKL](https://software.intel.com/en-us/onemkl-linux-developer-guide-improving-performance-with-threading) and [OpenBLAS](https://github.com/xianyi/OpenBLAS/wiki/faq#multi-threaded).
