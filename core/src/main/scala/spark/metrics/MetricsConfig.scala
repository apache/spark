package spark.metrics

import java.util.Properties
import java.io.FileInputStream

import scala.collection.mutable
import scala.util.matching.Regex

private [spark] class MetricsConfig(val configFile: String) {
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
  val DEFAULT_CONFIG_FILE = "conf/metrics.properties"
  val DEFAULT_PREFIX = "*"
  val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  
  def subProperties(prop: Properties, regex: Regex) = {
    val subProperties = new mutable.HashMap[String, Properties]
    
    import scala.collection.JavaConversions._
    prop.foreach { kv => 
      if (regex.findPrefixOf(kv._1) != None) {
        val regex(prefix, suffix) = kv._1
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2)
        println(">>>>>subProperties added  " + prefix + " " + suffix + " " + kv._2)
      }
    }
    
    subProperties
  }
}