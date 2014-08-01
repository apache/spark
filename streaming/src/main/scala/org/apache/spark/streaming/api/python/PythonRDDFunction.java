package org.apache.spark.streaming.api.python;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Time;

public interface PythonRDDFunction {
  JavaRDD<byte[]> call(JavaRDD<byte[]> rdd, long time);
}
