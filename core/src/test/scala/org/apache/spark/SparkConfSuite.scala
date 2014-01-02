package org.apache.spark

import org.scalatest.FunSuite

class SparkConfSuite extends FunSuite with LocalSparkContext {
  // This test uses the spark.conf in core/src/test/resources, which has a few test properties
  test("loading from spark.conf") {
    val conf = new SparkConf()
    assert(conf.get("spark.test.intTestProperty") === "1")
    assert(conf.get("spark.test.stringTestProperty") === "hi")
    // NOTE: we don't use list properties yet, but when we do, we'll have to deal with this syntax
    assert(conf.get("spark.test.listTestProperty") === "[a, b]")
  }

  // This test uses the spark.conf in core/src/test/resources, which has a few test properties
  test("system properties override spark.conf") {
    try {
      System.setProperty("spark.test.intTestProperty", "2")
      val conf = new SparkConf()
      assert(conf.get("spark.test.intTestProperty") === "2")
      assert(conf.get("spark.test.stringTestProperty") === "hi")
    } finally {
      System.clearProperty("spark.test.intTestProperty")
    }
  }

  test("initializing without loading defaults") {
    try {
      System.setProperty("spark.test.intTestProperty", "2")
      val conf = new SparkConf(false)
      assert(!conf.contains("spark.test.intTestProperty"))
      assert(!conf.contains("spark.test.stringTestProperty"))
    } finally {
      System.clearProperty("spark.test.intTestProperty")
    }
  }

  test("named set methods") {
    val conf = new SparkConf(false)

    conf.setMaster("local[3]")
    conf.setAppName("My app")
    conf.setSparkHome("/path")
    conf.setJars(Seq("a.jar", "b.jar"))
    conf.setExecutorEnv("VAR1", "value1")
    conf.setExecutorEnv(Seq(("VAR2", "value2"), ("VAR3", "value3")))

    assert(conf.get("spark.master") === "local[3]")
    assert(conf.get("spark.app.name") === "My app")
    assert(conf.get("spark.home") === "/path")
    assert(conf.get("spark.jars") === "a.jar,b.jar")
    assert(conf.get("spark.executorEnv.VAR1") === "value1")
    assert(conf.get("spark.executorEnv.VAR2") === "value2")
    assert(conf.get("spark.executorEnv.VAR3") === "value3")

    // Test the Java-friendly versions of these too
    conf.setJars(Array("c.jar", "d.jar"))
    conf.setExecutorEnv(Array(("VAR4", "value4"), ("VAR5", "value5")))
    assert(conf.get("spark.jars") === "c.jar,d.jar")
    assert(conf.get("spark.executorEnv.VAR4") === "value4")
    assert(conf.get("spark.executorEnv.VAR5") === "value5")
  }

  test("basic get and set") {
    val conf = new SparkConf(false)
    assert(conf.getAll.toSet === Set())
    conf.set("k1", "v1")
    conf.setAll(Seq(("k2", "v2"), ("k3", "v3")))
    assert(conf.getAll.toSet === Set(("k1", "v1"), ("k2", "v2"), ("k3", "v3")))
    conf.set("k1", "v4")
    conf.setAll(Seq(("k2", "v5"), ("k3", "v6")))
    assert(conf.getAll.toSet === Set(("k1", "v4"), ("k2", "v5"), ("k3", "v6")))
    assert(conf.contains("k1"), "conf did not contain k1")
    assert(!conf.contains("k4"), "conf contained k4")
    assert(conf.get("k1") === "v4")
    intercept[Exception] { conf.get("k4") }
    assert(conf.get("k4", "not found") === "not found")
    assert(conf.getOption("k1") === Some("v4"))
    assert(conf.getOption("k4") === None)
  }

  test("creating SparkContext without master and app name") {
    val conf = new SparkConf(false)
    intercept[SparkException] { sc = new SparkContext(conf) }
  }

  test("creating SparkContext without master") {
    val conf = new SparkConf(false).setAppName("My app")
    intercept[SparkException] { sc = new SparkContext(conf) }
  }

  test("creating SparkContext without app name") {
    val conf = new SparkConf(false).setMaster("local")
    intercept[SparkException] { sc = new SparkContext(conf) }
  }

  test("creating SparkContext with both master and app name") {
    val conf = new SparkConf(false).setMaster("local").setAppName("My app")
    sc = new SparkContext(conf)
    assert(sc.master === "local")
    assert(sc.appName === "My app")
  }

  test("SparkContext property overriding") {
    val conf = new SparkConf(false).setMaster("local").setAppName("My app")
    sc = new SparkContext("local[2]", "My other app", conf)
    assert(sc.master === "local[2]")
    assert(sc.appName === "My other app")
  }
}
