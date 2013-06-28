package spark.metrics

import java.util.Properties
import java.io.{File, FileInputStream}

import scala.collection.mutable
import scala.util.matching.Regex

private [spark] class MetricsConfig(val configFile: String) {
  val properties = new Properties()
  // Add default properties in case there's no properties file
  MetricsConfig.setDefaultProperties(properties)
  
  val confFile = new File(configFile)
  if (confFile.exists()) {
	var fis: FileInputStream = null
    try {
      fis = new FileInputStream(configFile)
      properties.load(fis)
    } finally {
      fis.close()
    }
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

private[spark] object MetricsConfig {
  val DEFAULT_PREFIX = "*"
  val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  
  def setDefaultProperties(prop: Properties) {
    prop.setProperty("*.sink.jmx.enabled", "default")
    prop.setProperty("*.source.jvm.class", "spark.metrics.source.JvmSource")
  }
  
  def subProperties(prop: Properties, regex: Regex) = {
    val subProperties = new mutable.HashMap[String, Properties]
    
    import scala.collection.JavaConversions._
    prop.foreach { kv => 
      if (regex.findPrefixOf(kv._1) != None) {
        val regex(prefix, suffix) = kv._1
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2)
      }
    }
    
    subProperties
  }
}