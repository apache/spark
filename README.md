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

To develop SparkR, you can build the scala package and the R package using

    ./install-dev.sh

If you wish to try out the package directly from github, you can use `install_git` from `devtools`

    library(devtools)
    install_git("https://github.com/amplab-extras/SparkR-pkg.git",
                subdir="pkg")

## Running sparkR
If you have cloned and built SparkR, you can start using it by launching the SparkR
shell with

    ./sparkR

If you have installed it directly from github, you can include the SparkR
package and then initialize a SparkContext. For example to run with a local
Spark master you can launch R and then run

    library(SparkR)
    sc <- sparkR.init(master="local")

SparkR also comes with several sample programs in the `examples` directory.
To run one of them, use `./sparkR <filename> <args>`. For example:

    ./sparkR examples/pi.R local[2]  

You can also run the unit-tests for SparkR by running

    ./run-tests.sh
