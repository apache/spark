package org.apache.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Hadoop Configuration for the Hadoop code (e.g. file systems).
 */
class SparkHadoopConf {


}

object SparkHadoopConf {
  private var hadoopConf: ThreadLocal[Configuration] = _
  def apply(conf: SparkConf): SparkHadoopConf = {
    hadoopConf = new ThreadLocal[Configuration]() {
      override def initialValue: Configuration = {
        SparkHadoopUtil.get.newConfiguration(conf)
      }
    }
    new SparkHadoopConf
  }

  def get(): ThreadLocal[Configuration] = hadoopConf

  def newHadoopConf(map: scala.collection.Map[String, String], conf: Configuration)
  : Configuration = {
    new Configuration()
  }
}
