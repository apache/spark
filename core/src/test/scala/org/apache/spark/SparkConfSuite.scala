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

import org.scalatest.FunSuite
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import org.apache.spark.util.ResetSystemProperties
import com.esotericsoftware.kryo.Kryo

class SparkConfSuite extends FunSuite with LocalSparkContext with ResetSystemProperties {
  test("loading from system properties") {
    System.setProperty("spark.test.testProperty", "2")
    val conf = new SparkConf()
    assert(conf.get("spark.test.testProperty") === "2")
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

}

class Class1 {}
class Class2 {}
class Class3 {}

class CustomRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Class2])
  }
}
