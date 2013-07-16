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
    val conf = new MetricsConfig(Option("dummy-file"))
    conf.initialize()

    assert(conf.properties.size() === 0)
    assert(conf.properties.getProperty("test-for-dummy") === null)

    val property = conf.getInstance("random")
    assert(property.size() === 0)
  }

  test("MetricsConfig with properties set") {
    val conf = new MetricsConfig(Option(filePath))
    conf.initialize()

    val masterProp = conf.getInstance("master")
    assert(masterProp.size() === 3)
    assert(masterProp.getProperty("sink.console.period") === "20")
    assert(masterProp.getProperty("sink.console.unit") === "minutes")
    assert(masterProp.getProperty("source.jvm.class") === "spark.metrics.source.JvmSource")

    val workerProp = conf.getInstance("worker")
    assert(workerProp.size() === 3)
    assert(workerProp.getProperty("sink.console.period") === "10")
    assert(workerProp.getProperty("sink.console.unit") === "seconds")
    assert(masterProp.getProperty("source.jvm.class") === "spark.metrics.source.JvmSource")
  }

  test("MetricsConfig with subProperties") {
    val conf = new MetricsConfig(Option(filePath))
    conf.initialize()

    val propCategories = conf.propertyCategories
    assert(propCategories.size === 2)

    val masterProp = conf.getInstance("master")
    val sourceProps = conf.subProperties(masterProp, MetricsSystem.SOURCE_REGEX)
    assert(sourceProps.size === 1)
    assert(sourceProps("jvm").getProperty("class") === "spark.metrics.source.JvmSource")

    val sinkProps = conf.subProperties(masterProp, MetricsSystem.SINK_REGEX)
    assert(sinkProps.size === 1)
    assert(sinkProps.contains("console"))

    val consoleProps = sinkProps("console")
    assert(consoleProps.size() === 2)
  }
}
