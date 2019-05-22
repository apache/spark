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
import scala.util.{Random, Try}

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.internal.config._
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.internal.config.Network._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.serializer.{JavaSerializer, KryoRegistrator, KryoSerializer}
import org.apache.spark.util.{ResetSystemProperties, RpcUtils, Utils}

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
    assert(conf.get(JARS) === Seq("a.jar", "b.jar"))
    assert(conf.get("spark.executorEnv.VAR1") === "value1")
    assert(conf.get("spark.executorEnv.VAR2") === "value2")
    assert(conf.get("spark.executorEnv.VAR3") === "value3")

    // Test the Java-friendly versions of these too
    conf.setJars(Array("c.jar", "d.jar"))
    conf.setExecutorEnv(Array(("VAR4", "value4"), ("VAR5", "value5")))
    assert(conf.get(JARS) === Seq("c.jar", "d.jar"))
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
    executor.scheduleAtFixedRate(
      () => System.setProperty("spark.5425." + Random.nextInt(), Random.nextInt().toString),
      0, 1, TimeUnit.MILLISECONDS)

    try {
      val t0 = System.nanoTime()
      while ((System.nanoTime() - t0) < TimeUnit.SECONDS.toNanos(1)) {
        val conf = Try(new SparkConf(loadDefaults = true))
        assert(conf.isSuccess)
      }
    } finally {
      executor.shutdownNow()
      val sysProps = System.getProperties
      for (key <- sysProps.stringPropertyNames().asScala if key.startsWith("spark.5425."))
        sysProps.remove(key)
    }
  }

  test("register kryo classes through registerKryoClasses") {
    val conf = new SparkConf().set(KRYO_REGISTRATION_REQUIRED, true)

    conf.registerKryoClasses(Array(classOf[Class1], classOf[Class2]))
    assert(conf.get(KRYO_CLASSES_TO_REGISTER).toSet ===
      Seq(classOf[Class1].getName, classOf[Class2].getName).toSet)

    conf.registerKryoClasses(Array(classOf[Class3]))
    assert(conf.get(KRYO_CLASSES_TO_REGISTER).toSet ===
      Seq(classOf[Class1].getName, classOf[Class2].getName, classOf[Class3].getName).toSet)

    conf.registerKryoClasses(Array(classOf[Class2]))
    assert(conf.get(KRYO_CLASSES_TO_REGISTER).toSet ===
      Seq(classOf[Class1].getName, classOf[Class2].getName, classOf[Class3].getName).toSet)

    // Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
    // blow up.
    val serializer = new KryoSerializer(conf)
    serializer.newInstance().serialize(new Class1())
    serializer.newInstance().serialize(new Class2())
    serializer.newInstance().serialize(new Class3())
  }

  test("register kryo classes through registerKryoClasses and custom registrator") {
    val conf = new SparkConf().set(KRYO_REGISTRATION_REQUIRED, true)

    conf.registerKryoClasses(Array(classOf[Class1]))
    assert(conf.get(KRYO_CLASSES_TO_REGISTER).toSet === Seq(classOf[Class1].getName).toSet)

    conf.set(KRYO_USER_REGISTRATORS, classOf[CustomRegistrator].getName)

    // Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
    // blow up.
    val serializer = new KryoSerializer(conf)
    serializer.newInstance().serialize(new Class1())
    serializer.newInstance().serialize(new Class2())
  }

  test("register kryo classes through conf") {
    val conf = new SparkConf().set(KRYO_REGISTRATION_REQUIRED, true)
    conf.set(KRYO_CLASSES_TO_REGISTER, Seq("java.lang.StringBuffer"))
    conf.set(SERIALIZER, classOf[KryoSerializer].getName)

    // Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
    // blow up.
    val serializer = new KryoSerializer(conf)
    serializer.newInstance().serialize(new StringBuffer())
  }

  test("deprecated configs") {
    val conf = new SparkConf()
    val newName = UPDATE_INTERVAL_S.key

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
    assert(conf.getSizeAsKb(KRYO_SERIALIZER_BUFFER_SIZE.key) === 1100)

    conf.set("spark.history.fs.cleaner.maxAge.seconds", "42")
    assert(conf.get(MAX_LOG_AGE_S) === 42L)

    conf.set("spark.scheduler.listenerbus.eventqueue.size", "84")
    assert(conf.get(LISTENER_BUS_EVENT_QUEUE_CAPACITY) === 84)

    conf.set("spark.yarn.access.namenodes", "testNode")
    assert(conf.get(KERBEROS_FILESYSTEMS_TO_ACCESS) === Array("testNode"))

    conf.set("spark.yarn.access.hadoopFileSystems", "testNode")
    assert(conf.get(KERBEROS_FILESYSTEMS_TO_ACCESS) === Array("testNode"))
  }

  test("akka deprecated configs") {
    val conf = new SparkConf()

    assert(!conf.contains(RPC_NUM_RETRIES))
    assert(!conf.contains(RPC_RETRY_WAIT))
    assert(!conf.contains(RPC_ASK_TIMEOUT))
    assert(!conf.contains(RPC_LOOKUP_TIMEOUT))

    conf.set("spark.akka.num.retries", "1")
    assert(RpcUtils.numRetries(conf) === 1)

    conf.set("spark.akka.retry.wait", "2")
    assert(RpcUtils.retryWaitMs(conf) === 2L)

    conf.set("spark.akka.askTimeout", "3")
    assert(RpcUtils.askRpcTimeout(conf).duration === 3.seconds)

    conf.set("spark.akka.lookupTimeout", "4")
    assert(RpcUtils.lookupRpcTimeout(conf).duration === 4.seconds)
  }

  test("SPARK-13727") {
    val conf = new SparkConf()
    // set the conf in the deprecated way
    conf.set("spark.io.compression.lz4.block.size", "12345")
    // get the conf in the recommended way
    assert(conf.get(IO_COMPRESSION_LZ4_BLOCKSIZE.key) === "12345")
    // we can still get the conf in the deprecated way
    assert(conf.get("spark.io.compression.lz4.block.size") === "12345")
    // the contains() also works as expected
    assert(conf.contains("spark.io.compression.lz4.block.size"))
    assert(conf.contains(IO_COMPRESSION_LZ4_BLOCKSIZE.key))
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

    conf.set(NETWORK_CRYPTO_ENABLED, true)
    intercept[IllegalArgumentException] {
      conf.validateSettings()
    }

    conf.set(NETWORK_CRYPTO_ENABLED, false)
    conf.set(SASL_ENCRYPTION_ENABLED, true)
    intercept[IllegalArgumentException] {
      conf.validateSettings()
    }

    conf.set(NETWORK_AUTH_ENABLED, true)
    conf.validateSettings()
  }

  test("spark.network.timeout should bigger than spark.executor.heartbeatInterval") {
    val conf = new SparkConf()
    conf.validateSettings()

    conf.set(NETWORK_TIMEOUT.key, "5s")
    intercept[IllegalArgumentException] {
      conf.validateSettings()
    }
  }

  test("SPARK-26998: SSL configuration not needed on executors") {
    val conf = new SparkConf(false)
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.trustStorePassword", "password")

    val filtered = conf.getAll.filter { case (k, _) => SparkConf.isExecutorStartupConf(k) }
    assert(filtered.isEmpty)
  }

  test("SPARK-27244 toDebugString redacts sensitive information") {
    val conf = new SparkConf(loadDefaults = false)
      .set("dummy.password", "dummy-password")
      .set("spark.hadoop.hive.server2.keystore.password", "1234")
      .set("spark.hadoop.javax.jdo.option.ConnectionPassword", "1234")
      .set("spark.regular.property", "regular_value")
    assert(conf.toDebugString ==
      s"""
        |dummy.password=${Utils.REDACTION_REPLACEMENT_TEXT}
        |spark.hadoop.hive.server2.keystore.password=${Utils.REDACTION_REPLACEMENT_TEXT}
        |spark.hadoop.javax.jdo.option.ConnectionPassword=${Utils.REDACTION_REPLACEMENT_TEXT}
        |spark.regular.property=regular_value
      """.stripMargin.trim)
  }

  val defaultIllegalValue = "SomeIllegalValue"
  val illegalValueTests : Map[String, (SparkConf, String) => Any] = Map(
    "getTimeAsSeconds" -> (_.getTimeAsSeconds(_)),
    "getTimeAsSeconds with default" -> (_.getTimeAsSeconds(_, defaultIllegalValue)),
    "getTimeAsMs" -> (_.getTimeAsMs(_)),
    "getTimeAsMs with default" -> (_.getTimeAsMs(_, defaultIllegalValue)),
    "getSizeAsBytes" -> (_.getSizeAsBytes(_)),
    "getSizeAsBytes with default string" -> (_.getSizeAsBytes(_, defaultIllegalValue)),
    "getSizeAsBytes with default long" -> (_.getSizeAsBytes(_, 0L)),
    "getSizeAsKb" -> (_.getSizeAsKb(_)),
    "getSizeAsKb with default" -> (_.getSizeAsKb(_, defaultIllegalValue)),
    "getSizeAsMb" -> (_.getSizeAsMb(_)),
    "getSizeAsMb with default" -> (_.getSizeAsMb(_, defaultIllegalValue)),
    "getSizeAsGb" -> (_.getSizeAsGb(_)),
    "getSizeAsGb with default" -> (_.getSizeAsGb(_, defaultIllegalValue)),
    "getInt" -> (_.getInt(_, 0)),
    "getLong" -> (_.getLong(_, 0L)),
    "getDouble" -> (_.getDouble(_, 0.0)),
    "getBoolean" -> (_.getBoolean(_, false))
  )

  illegalValueTests.foreach { case (name, getValue) =>
    test(s"SPARK-24337: $name throws an useful error message with key name") {
      val key = "SomeKey"
      val conf = new SparkConf()
      conf.set(key, "SomeInvalidValue")
      val thrown = intercept[IllegalArgumentException] {
        getValue(conf, key)
      }
      assert(thrown.getMessage.contains(key))
    }
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
