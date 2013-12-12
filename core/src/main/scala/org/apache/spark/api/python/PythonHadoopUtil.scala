package org.apache.spark.api.python

import org.apache.hadoop.conf.Configuration

/**
 * Utilities for working with Python objects -> Hadoop-related objects
 */
private[python] object PythonHadoopUtil {

  def mapToConf(map: java.util.HashMap[String, String]) = {
    import collection.JavaConversions._
    val conf = new Configuration()
    map.foreach{ case (k, v) => conf.set(k, v) }
    conf
  }

  /* Merges two configurations, keys from right overwrite any matching keys in left */
  def mergeConfs(left: Configuration, right: Configuration) = {
    import collection.JavaConversions._
    right.iterator().foreach(entry => left.set(entry.getKey, entry.getValue))
    left
  }

}
