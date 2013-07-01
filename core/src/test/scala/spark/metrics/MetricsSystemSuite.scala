package spark.metrics

import java.util.Properties
import java.io.{File, FileOutputStream}

import org.scalatest.{BeforeAndAfter, FunSuite}

import spark.metrics._

class MetricsSystemSuite extends FunSuite with BeforeAndAfter {
  var filePath: String = _
  
  before {
    filePath = getClass.getClassLoader.getResource("test_metrics_system.properties").getFile()
    System.setProperty("spark.metrics.conf.file", filePath)
  }
  
  test("MetricsSystem with default config") {
    val metricsSystem = MetricsSystem.createMetricsSystem("default")
    val sources = metricsSystem.sources
    val sinks = metricsSystem.sinks
    
    assert(sources.length === 1)
    assert(sinks.length === 1)
    assert(sources(0).sourceName === "jvm")
  }
  
  test("MetricsSystem with sources add") {
    val metricsSystem = MetricsSystem.createMetricsSystem("test")
    val sources = metricsSystem.sources
    val sinks = metricsSystem.sinks
    
    assert(sources.length === 1)
    assert(sinks.length === 2)
    
    val source = new spark.deploy.master.MasterInstrumentation(null)
    metricsSystem.registerSource(source)
    assert(sources.length === 2)
  }  
}
