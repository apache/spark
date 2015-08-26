#!/bin/bash -x

export LD_LIBRARY_PATH=${HOME}/levyx-spark/xenon/dist/lib
git clone https://github.com/semad/spark.git
#echo "##################### make -C "
#make -C levyx-spark/xenon/dist
pushd ./spark
echo "##################### sbt assembly"
sbt assembly
#echo "##################### sbt paraquet"
#sbt "test-only org.apache.spark.sql.parquet.*"
#echo "##################### sbt xenon"
#sbt "test-only org.apache.spark.sql.xenon.*"
