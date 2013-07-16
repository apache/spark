package spark.metrics

import java.util.Properties
import java.io.{File, FileOutputStream}

import org.scalatest.{BeforeAndAfter, FunSuite}

import spark.metrics._

class MetricsSystemSuite extends FunSuite with BeforeAndAfter {
  var filePath: String = _

  before {
    filePath = getClass.getClassLoader.getResource("test_metrics_system.properties").getFile()
    System.setProperty("spark.metrics.conf", filePath)
  }

  test("MetricsSystem with default config") {
    val metricsSystem = MetricsSystem.createMetricsSystem("default")
    val sources = metricsSystem.sources
    val sinks = metricsSystem.sinks

    assert(sources.length === 0)
    assert(sinks.length === 0)
  }

  test("MetricsSystem with sources add") {
    val metricsSystem = MetricsSystem.createMetricsSystem("test")
    val sources = metricsSystem.sources
    val sinks = metricsSystem.sinks

    assert(sources.length === 0)
    assert(sinks.length === 1)

    val source = new spark.deploy.master.MasterSource(null)
    metricsSystem.registerSource(source)
    assert(sources.length === 1)
  }
}
