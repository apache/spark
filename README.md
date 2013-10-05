# R on Spark

## Building

R on Spark requires the R package `rJava` to be installed. To install `rJava`,
you can run the following command in R:

    install.packages("rJava")

To run R on Spark, first build Spark using `sbt/sbt assembly`. Following that
compile the SparkR package by running

    make -C R

Once you have built Spark and the SparkR package, you can start using by
launching the SparkR shell with

    ./sparkR

SparkR also comes with several sample programs in the `R/examples` directory. 
To run one of them, use `./sparkR <filename> <args>`. For example:

    ./sparkR R/examples/pi.R local[2]  
