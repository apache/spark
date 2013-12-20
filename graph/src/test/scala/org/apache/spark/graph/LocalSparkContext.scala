package org.apache.spark.graph

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkContext


/**
 * Provides a method to run tests against a {@link SparkContext} variable that is correctly stopped
 * after each test.
*/
trait LocalSparkContext {
  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

  /** Runs `f` on a new SparkContext and ensures that it is stopped afterwards. */
  def withSpark[T](f: SparkContext => T) = {
    val sc = new SparkContext("local", "test")
    try {
      f(sc)
    } finally {
      sc.stop()
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
    }
  }
}
