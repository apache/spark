package org.apache.spark.streaming.api.python;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Time;

/*
 * Interface for py4j callback function.
 * This interface is related to pyspark.streaming.dstream.DStream.foreachRDD .
 */
public interface PythonRDDFunction {
  JavaRDD<byte[]> call(JavaRDD<byte[]> rdd, long time);
}