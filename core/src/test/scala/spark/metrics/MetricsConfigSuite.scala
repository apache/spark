package spark.metrics

import java.util.Properties
import java.io.{File, FileOutputStream}

import org.scalatest.{BeforeAndAfter, FunSuite}

import spark.metrics._

class MetricsConfigSuite extends FunSuite with BeforeAndAfter {
  var filePath: String = _
  
  before {
    filePath = getClass.getClassLoader.getResource("test_metrics_config.properties").getFile()
  }

  test("MetricsConfig with default properties") {
    val conf = new MetricsConfig("dummy-file")
    assert(conf.properties.size() === 2)
    assert(conf.properties.getProperty("*.sink.jmx.enabled") === "default")
    assert(conf.properties.getProperty("*.source.jvm.class") === "spark.metrics.source.JvmSource")
    assert(conf.properties.getProperty("test-for-dummy") === null)
    
    val property = conf.getInstance("random")
    assert(property.size() === 2)
    assert(property.getProperty("sink.jmx.enabled") === "default")
    assert(property.getProperty("source.jvm.class") === "spark.metrics.source.JvmSource")
  }
  
  test("MetricsConfig with properties set") {
    val conf = new MetricsConfig(filePath)
    
    val masterProp = conf.getInstance("master")
    assert(masterProp.size() === 4)
    assert(masterProp.getProperty("sink.console.period") === "20")
    assert(masterProp.getProperty("sink.console.unit") === "minute")
    assert(masterProp.getProperty("sink.jmx.enabled") === "default")
    assert(masterProp.getProperty("source.jvm.class") == "spark.metrics.source.JvmSource")
    
    val workerProp = conf.getInstance("worker")
    assert(workerProp.size() === 4)
    assert(workerProp.getProperty("sink.console.period") === "10")
    assert(workerProp.getProperty("sink.console.unit") === "second")
  }
  
  test("MetricsConfig with subProperties") {
    val conf = new MetricsConfig(filePath)
    
    val propCategories = conf.propertyCategories
    assert(propCategories.size === 2)
    
    val masterProp = conf.getInstance("master")
    val sourceProps = MetricsConfig.subProperties(masterProp, MetricsSystem.SOURCE_REGEX)
    assert(sourceProps.size === 1)
    assert(sourceProps("jvm").getProperty("class") === "spark.metrics.source.JvmSource")
    
    val sinkProps = MetricsConfig.subProperties(masterProp, MetricsSystem.SINK_REGEX)
    assert(sinkProps.size === 2)
    assert(sinkProps.contains("console"))
    assert(sinkProps.contains("jmx"))
    
    val consoleProps = sinkProps("console")
    assert(consoleProps.size() === 2)
    
    val jmxProps = sinkProps("jmx")
    assert(jmxProps.size() === 1)
  } 
}
