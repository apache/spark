package spark

import org.scalatest.BeforeAndAfterAll


class ShuffleNettySuite extends ShuffleSuite with BeforeAndAfterAll {

  // This test suite should run all tests in ShuffleSuite with Netty shuffle mode.

  override def beforeAll(configMap: Map[String, Any]) {
    System.setProperty("spark.shuffle.use.netty", "true")
  }

  override def afterAll(configMap: Map[String, Any]) {
    System.setProperty("spark.shuffle.use.netty", "false")
  }
}
