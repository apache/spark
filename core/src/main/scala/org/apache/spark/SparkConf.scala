package org.apache.spark

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap

import com.typesafe.config.ConfigFactory

private[spark] class SparkConf(loadClasspathRes: Boolean = true) extends Serializable {
  @transient lazy val config = ConfigFactory.systemProperties()
    .withFallback(ConfigFactory.parseResources("spark.conf"))
  // TODO this should actually be synchronized
  private val configMap = TrieMap[String, String]()

  if (loadClasspathRes && !config.entrySet().isEmpty) {
    for (e <- config.entrySet()) {
      configMap += ((e.getKey, e.getValue.unwrapped().toString))
    }
  }

  def setMasterUrl(master: String) = {
    if (master != null)
      configMap += (("spark.master", master))
    this
  }

  def setAppName(name: String) = {
    if (name != null)
      configMap += (("spark.appName", name))
    this
  }

  def setJars(jars: Seq[String]) = {
    if (!jars.isEmpty)
      configMap += (("spark.jars", jars.mkString(",")))
    this
  }

  def set(k: String, value: String) = {
    configMap += ((k, value))
    this
  }

  def setSparkHome(home: String) = {
    if (home != null)
      configMap += (("spark.home", home))
    this
  }

  def set(map: Seq[(String, String)]) = {
    if (map != null && !map.isEmpty)
      configMap ++= map
    this
  }

  def get(k: String): String = {
    configMap(k)
  }

  def getAllConfiguration = configMap.clone.entrySet().iterator

  def getOrElse(k: String, defaultValue: String): String = {
    configMap.getOrElse(k, defaultValue)
  }

  override def clone: SparkConf = {
    val conf = new SparkConf(false)
    conf.set(configMap.toSeq)
    conf
  }

}
