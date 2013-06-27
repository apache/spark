package spark.metrics

import java.util.Properties
import java.io.FileInputStream

import scala.collection.mutable
import scala.util.matching.Regex

class MetricsConfig(val configFile: String) {
  val properties = new Properties()
  var fis: FileInputStream = _
  
  try {
    fis = new FileInputStream(configFile)
    properties.load(fis)
  } finally {
    fis.close()
  }
  
  val propertyCategories = MetricsConfig.subProperties(properties, MetricsConfig.INSTANCE_REGEX)
  if (propertyCategories.contains(MetricsConfig.DEFAULT_PREFIX)) {
    import scala.collection.JavaConversions._
    val defaultProperty = propertyCategories(MetricsConfig.DEFAULT_PREFIX)
    for ((inst, prop) <- propertyCategories; p <- defaultProperty
    		if inst != MetricsConfig.DEFAULT_PREFIX; if prop.getProperty(p._1) == null) {
      prop.setProperty(p._1, p._2)
    }
  }
  
  def getInstance(inst: String) = {
    propertyCategories.get(inst) match {
      case Some(s) => s
      case None => propertyCategories(MetricsConfig.DEFAULT_PREFIX)
    }
  }
}

object MetricsConfig {
  val DEFAULT_CONFIG_FILE = "/home/jerryshao/project/sotc_cloud-spark/conf/metrics.properties"
  val DEFAULT_PREFIX = "*"
  val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  
  def subProperties(prop: Properties, regex: Regex) = {
    val subProperties = new mutable.HashMap[String, Properties]
    
    import scala.collection.JavaConversions._
    prop.foreach { kv => 
      val regex(a, b) = kv._1
      subProperties.getOrElseUpdate(a, new Properties).setProperty(b, kv._2)
      println(">>>>>subProperties added  " + a + " " + b + " " + kv._2)
    }
    
    subProperties
  }
}