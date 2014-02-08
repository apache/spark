package org.apache.spark.mllib.util

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkContext

trait LocalSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}
