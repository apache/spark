# R on Spark

SparkR is an R package that provides a light-weight frontend to use Spark from
R.

## Building

### Installing Spark
SparkR requires Scala 2.10 and Spark version >= 0.9.0. As Spark 0.9.0 has not
been released yet, you will need to clone the Spark master branch and
run `sbt/sbt publish-local` to publish 0.9.0-SNAPSHOT.

### Building SparkR
SparkR requires the R package `rJava` to be installed. To install `rJava`,
you can run the following command in R:

    install.packages("rJava")

To run SparkR, first build the scala package using

    sbt/sbt assembly

Following that compile the R package by running

    make

## Running sparkR
Once you have built SparkR, you can start using it by launching the SparkR
shell with

    ./sparkR

SparkR also comes with several sample programs in the `examples` directory.
To run one of them, use `./sparkR <filename> <args>`. For example:

    ./sparkR examples/pi.R local[2]  

You can also run the unit-tests for SparkR by running

    ./sparkR pkg/inst/tests/run-all.R
