package org.apache.spark

import org.scalatest.FunSuite

/**
 * Test if platform extension was initialized.
 */
class PlatformExtensionSuite extends FunSuite with LocalSparkContext {

  test("extension is started on all executors in cluster") {
    val conf = new SparkConf()
    conf.setMaster("local-cluster[3,2,512]")
    conf.setAppName("test")
    conf.addExtension("org.apache.spark.TestPlatformExt")

    // Create a jar containing extension classes
    val jar = createJarWithExtension()
    conf.setJars(List(jar.getPath))

    // Create spark context based on constructed configuration
    sc = new SparkContext(conf)
    // Create dataset distributed over cluster of 3 workers
    // and verify that Status singleton is initialized
    val executorStatus = sc.parallelize(1 to 3, 3).map(_ => {
      val statusKlazz = Class.forName("org.apache.spark.ExtStatus")
      val method = statusKlazz.getMethod("initialized")
      method.invoke(null)
    }).collect()

    assert(executorStatus === Array(true, true, true))
    // Status singleton should not be initialized inside driver
    assert(!ExtStatus.initialized, "Status singleton should not be initialized inside driver!")
    // Terminate Spark context
    resetSparkContext()
  }

  test("extension is not started on executors if not specified by Spark config") {
    val conf = new SparkConf()
    conf.setMaster("local-cluster[3,2,512]")
    conf.setAppName("test")

    // Create a jar containing extension classes
    val jar = createJarWithExtension()
    conf.setJars(List(jar.getPath))

    // Create spark context based on constructed configuration
    sc = new SparkContext(conf)
    // Create dataset distributed over cluster of 3 workers
    val executorStatus = sc.parallelize(1 to 3, 3).map(_ => {
      val statusKlazz = Class.forName("org.apache.spark.ExtStatus")
      val method = statusKlazz.getMethod("initialized")
      method.invoke(null)
    }).collect()

    // Status should not be initialized on executors
    assert(executorStatus === Array(false, false, false))
    // Neither locally
    assert(!ExtStatus.initialized, "Status singleton should not be initialized inside driver!")
    // Terminate Spark context
    resetSparkContext()
  }

  def createJarWithExtension() =
    TestUtils.createJarWithExistingClasses(
      List(
        "org.apache.spark.TestPlatformExt",
        "org.apache.spark.ExtStatus", "org.apache.spark.ExtStatus$"),
      this.getClass.getClassLoader)
}

/** Simple platform extension flipping flag in ExtStatus singleton */
class TestPlatformExt extends PlatformExtension {
  override def start(conf: SparkConf): Unit = ExtStatus.init()
  override def stop(conf: SparkConf): Unit = { }
  override def desc: String = "test"
}

/** Flag singleton exposing status of extension */
object ExtStatus {
  private var flag = false
  def init(): Unit = { println("A"); flag = true }
  def initialized = flag
}
