package org.apache.spark.api.python

import org.apache.hadoop.conf.Configuration

/**
 * Utilities for working with Python objects -> Hadoop-related objects
 */
private[python] object PythonHadoopUtil {

  def mapToConf(map: java.util.Map[String, String]) = {
    import collection.JavaConversions._
    val conf = new Configuration()
    map.foreach{ case (k, v) => conf.set(k, v) }
    conf
  }

  /** Merges two configurations, returns a copy of left with keys from right overwriting any matching keys in left */
  def mergeConfs(left: Configuration, right: Configuration) = {
    import collection.JavaConversions._
    val copy = new Configuration(left)
    right.iterator().foreach(entry => copy.set(entry.getKey, entry.getValue))
    copy
  }

}
