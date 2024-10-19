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

import scala.jdk.CollectionConverters._
import scala.util.{Random, Try}

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.internal.config._
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.internal.config.Network._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.resource.ResourceID
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.serializer.{JavaSerializer, KryoRegistrator, KryoSerializer}
import org.apache.spark.util.{ResetSystemProperties, Utils}

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

  test("basic getAllWithPrefix") {
    val prefix = "spark.prefix."
    val conf = new SparkConf(false)
    conf.set("spark.prefix.main.suffix", "v1")
    assert(conf.getAllWithPrefix(prefix).toSet ===
      Set(("main.suffix", "v1")))

    conf.set("spark.prefix.main2.suffix", "v2")
    conf.set("spark.prefix.main3.extra1.suffix", "v3")
    conf.set("spark.notMatching.main4", "v4")

    assert(conf.getAllWithPrefix(prefix).toSet ===
      Set(("main.suffix", "v1"), ("main2.suffix", "v2"), ("main3.extra1.suffix", "v3")))
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

    conf.set(KRYO_USER_REGISTRATORS, Seq(classOf[CustomRegistrator].getName))

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
    val conf = new SparkConf(false)
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

  test("SPARK-26998: SSL passwords not needed on executors") {
    val conf = new SparkConf(false)
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.trustStorePassword", "password")

    val filtered = conf.getAll.filter { case (k, _) => SparkConf.isExecutorStartupConf(k) }
    // Only the enabled flag should propagate
    assert(filtered.length == 1)
    assert(filtered(0)._1 == "spark.ssl.enabled")
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

  test("SPARK-28355: Use Spark conf for threshold at which UDFs are compressed by broadcast") {
    val conf = new SparkConf()

    // Check the default value
    assert(conf.get(BROADCAST_FOR_UDF_COMPRESSION_THRESHOLD) === 1L * 1024 * 1024)

    // Set the conf
    conf.set(BROADCAST_FOR_UDF_COMPRESSION_THRESHOLD, 1L * 1024)

    // Verify that it has been set properly
    assert(conf.get(BROADCAST_FOR_UDF_COMPRESSION_THRESHOLD) === 1L * 1024)
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

  test("get task resource requirement from config") {
    val conf = new SparkConf()
    conf.set(TASK_GPU_ID.amountConf, "2")
    conf.set(TASK_FPGA_ID.amountConf, "1")
    var taskResourceRequirement =
      parseResourceRequirements(conf, SPARK_TASK_PREFIX)
        .map(req => (req.resourceName, req.amount)).toMap

    assert(taskResourceRequirement.size == 2)
    assert(taskResourceRequirement(GPU) == 2)
    assert(taskResourceRequirement(FPGA) == 1)

    conf.remove(TASK_FPGA_ID.amountConf)
    // Ignore invalid prefix
    conf.set(new ResourceID("spark.invalid.prefix", FPGA).amountConf, "1")
    taskResourceRequirement =
      parseResourceRequirements(conf, SPARK_TASK_PREFIX)
        .map(req => (req.resourceName, req.amount)).toMap
    assert(taskResourceRequirement.size == 1)
    assert(taskResourceRequirement.get(FPGA).isEmpty)
  }

  test("test task resource requirement with 0 amount") {
    val conf = new SparkConf()
    conf.set(TASK_GPU_ID.amountConf, "2")
    conf.set(TASK_FPGA_ID.amountConf, "0")
    val taskResourceRequirement =
      parseResourceRequirements(conf, SPARK_TASK_PREFIX)
        .map(req => (req.resourceName, req.amount)).toMap

    assert(taskResourceRequirement.size == 1)
    assert(taskResourceRequirement(GPU) == 2)
  }


  test("Ensure that we can configure fractional resources for a task") {
    val ratioSlots = Seq(
      (0.10, 10), (0.11, 9), (0.125, 8), (0.14, 7), (0.16, 6),
      (0.20, 5), (0.25, 4), (0.33, 3), (0.5, 2), (1.0, 1),
      // if the amount is fractional greater than 0.5 and less than 1.0 we throw
      (0.51, 1), (0.9, 1),
      // if the amount is greater than one is not whole, we throw
      (1.5, 0), (2.5, 0),
      // it's ok if the amount is whole, and greater than 1
      // parts are 1 because we get a whole part of a resource
      (2.0, 1), (3.0, 1), (4.0, 1))
    ratioSlots.foreach {
      case (ratio, slots) =>
        val conf = new SparkConf()
        conf.set(TASK_GPU_ID.amountConf, ratio.toString)
        if (ratio > 1.0 && ratio % 1 != 0) {
          assertThrows[SparkException] {
            parseResourceRequirements(conf, SPARK_TASK_PREFIX)
          }
        } else {
          val reqs = parseResourceRequirements(conf, SPARK_TASK_PREFIX)
          assert(reqs.size == 1)
          assert(reqs.head.amount == Math.ceil(ratio).toInt)
          assert(reqs.head.numParts == slots)
        }
    }
  }

  test("Non-task resources are never fractional") {
    val ratioSlots = Seq(
      // if the amount provided is not a whole number, we throw
      (0.25, 0), (0.5, 0), (1.5, 0),
      // otherwise we are successful at parsing resources
      (1.0, 1), (2.0, 2), (3.0, 3))
    ratioSlots.foreach {
      case (ratio, slots) =>
        val conf = new SparkConf()
        conf.set(EXECUTOR_GPU_ID.amountConf, ratio.toString)
        if (ratio % 1 != 0) {
          assertThrows[SparkException] {
            parseResourceRequirements(conf, SPARK_EXECUTOR_PREFIX)
          }
        } else {
          val reqs = parseResourceRequirements(conf, SPARK_EXECUTOR_PREFIX)
          assert(reqs.size == 1)
          assert(reqs.head.amount == slots)
          assert(reqs.head.numParts == 1)
        }
    }
  }

  test("SPARK-44650: spark.executor.defaultJavaOptions Check illegal java options") {
    val conf = new SparkConf()
    conf.validateSettings()
    conf.set(EXECUTOR_JAVA_OPTIONS.key, "-Dspark.foo=bar")
    intercept[Exception] {
      conf.validateSettings()
    }
    conf.remove(EXECUTOR_JAVA_OPTIONS.key)
    conf.set("spark.executor.defaultJavaOptions", "-Dspark.foo=bar")
    intercept[Exception] {
      conf.validateSettings()
    }
  }
}

class Class1 {}
class Class2 {}
class Class3 {}

class CustomRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Class2])
  }
}
