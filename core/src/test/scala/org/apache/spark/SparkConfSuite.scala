/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Random, Try}

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.internal.config._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.serializer.{JavaSerializer, KryoRegistrator, KryoSerializer}
import org.apache.spark.util.{ResetSystemProperties, RpcUtils}

class SparkConfSuite extends SparkFunSuite with LocalSparkContext with ResetSystemProperties {
  test("Test byteString conversion") {
    val conf = new SparkConf()
    // Simply exercise the API, we don't need a complete conversion test since that's handled in
    // UtilsSuite.scala
    assert(conf.getSizeAsBytes("fake", "1k") === ByteUnit.KiB.toBytes(1))
    assert(conf.getSizeAsKb("fake", "1k") === ByteUnit.KiB.toKiB(1))
    assert(conf.getSizeAsMb("fake", "1k") === ByteUnit.KiB.toMiB(1))
    assert(conf.getSizeAsGb("fake", "1k") === ByteUnit.KiB.toGiB(1))
  }

  test("Test timeString conversion") {
    val conf = new SparkConf()
    // Simply exercise the API, we don't need a complete conversion test since that's handled in
    // UtilsSuite.scala
    assert(conf.getTimeAsMs("fake", "1ms") === TimeUnit.MILLISECONDS.toMillis(1))
    assert(conf.getTimeAsSeconds("fake", "1000ms") === TimeUnit.MILLISECONDS.toSeconds(1000))
  }

  test("loading from system properties") {
    System.setProperty("spark.test.testProperty", "2")
    System.setProperty("nonspark.test.testProperty", "0")
    val conf = new SparkConf()
    assert(conf.get("spark.test.testProperty") === "2")
    assert(!conf.contains("nonspark.test.testProperty"))
  }

  test("initializing without loading defaults") {
    System.setProperty("spark.test.testProperty", "2")
    val conf = new SparkConf(false)
    assert(!conf.contains("spark.test.testProperty"))
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

  test("nested property names") {
    // This wasn't supported by some external conf parsing libraries
    System.setProperty("spark.test.a", "a")
    System.setProperty("spark.test.a.b", "a.b")
    System.setProperty("spark.test.a.b.c", "a.b.c")
    val conf = new SparkConf()
    assert(conf.get("spark.test.a") === "a")
    assert(conf.get("spark.test.a.b") === "a.b")
    assert(conf.get("spark.test.a.b.c") === "a.b.c")
    conf.set("spark.test.a.b", "A.B")
    assert(conf.get("spark.test.a") === "a")
    assert(conf.get("spark.test.a.b") === "A.B")
    assert(conf.get("spark.test.a.b.c") === "a.b.c")
  }

  test("Thread safeness - SPARK-5425") {
    val executor = Executors.newSingleThreadScheduledExecutor()
    val sf = executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit =
        System.setProperty("spark.5425." + Random.nextInt(), Random.nextInt().toString)
    }, 0, 1, TimeUnit.MILLISECONDS)

    try {
      val t0 = System.currentTimeMillis()
      while ((System.currentTimeMillis() - t0) < 1000) {
        val conf = Try(new SparkConf(loadDefaults = true))
        assert(conf.isSuccess === true)
      }
    } finally {
      executor.shutdownNow()
      val sysProps = System.getProperties
      for (key <- sysProps.stringPropertyNames().asScala if key.startsWith("spark.5425."))
        sysProps.remove(key)
    }
  }

  test("register kryo classes through registerKryoClasses") {
    val conf = new SparkConf().set("spark.kryo.registrationRequired", "true")

    conf.registerKryoClasses(Array(classOf[Class1], classOf[Class2]))
    assert(conf.get("spark.kryo.classesToRegister") ===
      classOf[Class1].getName + "," + classOf[Class2].getName)

    conf.registerKryoClasses(Array(classOf[Class3]))
    assert(conf.get("spark.kryo.classesToRegister") ===
      classOf[Class1].getName + "," + classOf[Class2].getName + "," + classOf[Class3].getName)

    conf.registerKryoClasses(Array(classOf[Class2]))
    assert(conf.get("spark.kryo.classesToRegister") ===
      classOf[Class1].getName + "," + classOf[Class2].getName + "," + classOf[Class3].getName)

    // Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
    // blow up.
    val serializer = new KryoSerializer(conf)
    serializer.newInstance().serialize(new Class1())
    serializer.newInstance().serialize(new Class2())
    serializer.newInstance().serialize(new Class3())
  }

  test("register kryo classes through registerKryoClasses and custom registrator") {
    val conf = new SparkConf().set("spark.kryo.registrationRequired", "true")

    conf.registerKryoClasses(Array(classOf[Class1]))
    assert(conf.get("spark.kryo.classesToRegister") === classOf[Class1].getName)

    conf.set("spark.kryo.registrator", classOf[CustomRegistrator].getName)

    // Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
    // blow up.
    val serializer = new KryoSerializer(conf)
    serializer.newInstance().serialize(new Class1())
    serializer.newInstance().serialize(new Class2())
  }

  test("register kryo classes through conf") {
    val conf = new SparkConf().set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.classesToRegister", "java.lang.StringBuffer")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)

    // Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
    // blow up.
    val serializer = new KryoSerializer(conf)
    serializer.newInstance().serialize(new StringBuffer())
  }

  test("deprecated configs") {
    val conf = new SparkConf()
    val newName = "spark.history.fs.update.interval"

    assert(!conf.contains(newName))

    conf.set("spark.history.updateInterval", "1")
    assert(conf.get(newName) === "1")

    conf.set("spark.history.fs.updateInterval", "2")
    assert(conf.get(newName) === "2")

    conf.set("spark.history.fs.update.interval.seconds", "3")
    assert(conf.get(newName) === "3")

    conf.set(newName, "4")
    assert(conf.get(newName) === "4")

    val count = conf.getAll.count { case (k, v) => k.startsWith("spark.history.") }
    assert(count === 4)

    conf.set("spark.yarn.applicationMaster.waitTries", "42")
    assert(conf.getTimeAsSeconds("spark.yarn.am.waitTime") === 420)

    conf.set("spark.kryoserializer.buffer.mb", "1.1")
    assert(conf.getSizeAsKb("spark.kryoserializer.buffer") === 1100)
  }

  test("akka deprecated configs") {
    val conf = new SparkConf()

    assert(!conf.contains("spark.rpc.numRetries"))
    assert(!conf.contains("spark.rpc.retry.wait"))
    assert(!conf.contains("spark.rpc.askTimeout"))
    assert(!conf.contains("spark.rpc.lookupTimeout"))

    conf.set("spark.akka.num.retries", "1")
    assert(RpcUtils.numRetries(conf) === 1)

    conf.set("spark.akka.retry.wait", "2")
    assert(RpcUtils.retryWaitMs(conf) === 2L)

    conf.set("spark.akka.askTimeout", "3")
    assert(RpcUtils.askRpcTimeout(conf).duration === (3 seconds))

    conf.set("spark.akka.lookupTimeout", "4")
    assert(RpcUtils.lookupRpcTimeout(conf).duration === (4 seconds))
  }

  test("SPARK-13727") {
    val conf = new SparkConf()
    // set the conf in the deprecated way
    conf.set("spark.io.compression.lz4.block.size", "12345")
    // get the conf in the recommended way
    assert(conf.get("spark.io.compression.lz4.blockSize") === "12345")
    // we can still get the conf in the deprecated way
    assert(conf.get("spark.io.compression.lz4.block.size") === "12345")
    // the contains() also works as expected
    assert(conf.contains("spark.io.compression.lz4.block.size"))
    assert(conf.contains("spark.io.compression.lz4.blockSize"))
    assert(conf.contains("spark.io.unknown") === false)
  }

  val serializers = Map(
    "java" -> new JavaSerializer(new SparkConf()),
    "kryo" -> new KryoSerializer(new SparkConf()))

  serializers.foreach { case (name, ser) =>
    test(s"SPARK-17240: SparkConf should be serializable ($name)") {
      val conf = new SparkConf()
      conf.set(DRIVER_CLASS_PATH, "${" + DRIVER_JAVA_OPTIONS.key + "}")
      conf.set(DRIVER_JAVA_OPTIONS, "test")

      val serializer = ser.newInstance()
      val bytes = serializer.serialize(conf)
      val deser = serializer.deserialize[SparkConf](bytes)

      assert(conf.get(DRIVER_CLASS_PATH) === deser.get(DRIVER_CLASS_PATH))
    }
  }

  test("encryption requires authentication") {
    val conf = new SparkConf()
    conf.validateSettings()

    conf.set(NETWORK_ENCRYPTION_ENABLED, true)
    intercept[IllegalArgumentException] {
      conf.validateSettings()
    }

    conf.set(NETWORK_ENCRYPTION_ENABLED, false)
    conf.set(SASL_ENCRYPTION_ENABLED, true)
    intercept[IllegalArgumentException] {
      conf.validateSettings()
    }

    conf.set(NETWORK_AUTH_ENABLED, true)
    conf.validateSettings()
  }

}

class Class1 {}
class Class2 {}
class Class3 {}

class CustomRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Class2])
  }
}
