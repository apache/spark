package spark.metrics

import java.util.Properties
import java.io.{File, FileInputStream}

import scala.collection.mutable.HashMap
import scala.util.matching.Regex

private[spark] class MetricsConfig(val configFile: String) {
  val properties = new Properties()
  val DEFAULT_PREFIX = "*"
  val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  var propertyCategories: HashMap[String, Properties] = null

  private def setDefaultProperties(prop: Properties) {
    prop.setProperty("*.sink.jmx.enabled", "default")
    prop.setProperty("*.source.jvm.class", "spark.metrics.source.JvmSource")
  }

  def initilize() {
    //Add default properties in case there's no properties file
    setDefaultProperties(properties)

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

    propertyCategories = subProperties(properties, INSTANCE_REGEX)
    if (propertyCategories.contains(DEFAULT_PREFIX)) {
      import scala.collection.JavaConversions._
      val defaultProperty = propertyCategories(DEFAULT_PREFIX)
      for ((inst, prop) <- propertyCategories; p <- defaultProperty
        if inst != DEFAULT_PREFIX; if prop.getProperty(p._1) == null) {
        prop.setProperty(p._1, p._2)
      }
    }
  }

  def subProperties(prop: Properties, regex: Regex): HashMap[String, Properties] = {
    val subProperties = new HashMap[String, Properties]
    import scala.collection.JavaConversions._
    prop.foreach { kv =>
      if (regex.findPrefixOf(kv._1) != None) {
        val regex(prefix, suffix) = kv._1
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2)
      }
    }
    subProperties
  }

  def getInstance(inst: String): Properties = {
    propertyCategories.get(inst) match {
      case Some(s) => s
      case None => propertyCategories(DEFAULT_PREFIX)
    }
  }
}

