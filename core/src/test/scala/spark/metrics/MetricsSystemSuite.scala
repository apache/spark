package spark.metrics

import java.util.Properties
import java.io.{File, FileOutputStream}

import org.scalatest.{BeforeAndAfter, FunSuite}

import spark.metrics._

class MetricsSystemSuite extends FunSuite with BeforeAndAfter {
  var filePath: String = _
  
  before {
    val props = new Properties()
    props.setProperty("*.sink.console.period", "10")
    props.setProperty("*.sink.console.unit", "second")
    props.setProperty("test.sink.console.class", "spark.metrics.sink.ConsoleSink")
    props.setProperty("test.sink.dummy.class", "spark.metrics.sink.DummySink")
    props.setProperty("test.source.dummy.class", "spark.metrics.source.DummySource")
    props.setProperty("test.sink.console.period", "20")
    props.setProperty("test.sink.console.unit", "minute")
    
    val dir = new File("/tmp")
    filePath = if (dir.isDirectory() && dir.exists() && dir.canWrite()) {
      "/tmp/test_metrics.properties" 
    } else {
      "./test_metrics.properties"
    }
    
    val os = new FileOutputStream(new File(filePath))    
    props.store(os, "for test")
    os.close()
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
  
  after {
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    }
  }
}