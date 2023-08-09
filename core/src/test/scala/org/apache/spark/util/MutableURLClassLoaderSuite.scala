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

package org.apache.spark.util

import java.net.URLClassLoader

import scala.collection.JavaConverters._

import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SparkContext, SparkException, SparkFunSuite, TestUtils}

class MutableURLClassLoaderSuite extends SparkFunSuite with Matchers {

  val urls2 = List(TestUtils.createJarWithClasses(
      classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
      toStringValue = "2")).toArray
  val urls = List(TestUtils.createJarWithClasses(
      classNames = Seq("FakeClass1"),
      classNamesWithBase = Seq(("FakeClass2", "FakeClass3")), // FakeClass3 is in parent
      toStringValue = "1",
      classpathUrls = urls2)).toArray

  val fileUrlsChild = List(TestUtils.createJarWithFiles(Map(
    "resource1" -> "resource1Contents-child",
    "resource2" -> "resource2Contents"))).toArray
  val fileUrlsParent = List(TestUtils.createJarWithFiles(Map(
    "resource1" -> "resource1Contents-parent"))).toArray

  test("child first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildFirstURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass2").getConstructor().newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    val fakeClass2 = classLoader.loadClass("FakeClass2").getConstructor().newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
    classLoader.close()
    parentLoader.close()
  }

  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new MutableURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass1").getConstructor().newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
    val fakeClass2 = classLoader.loadClass("FakeClass1").getConstructor().newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
    classLoader.close()
    parentLoader.close()
  }

  test("child first can fall back") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildFirstURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass3").getConstructor().newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
    classLoader.close()
    parentLoader.close()
  }

  test("child first can fail") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildFirstURLClassLoader(urls, parentLoader)
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("FakeClassDoesNotExist").getConstructor().newInstance()
    }
    classLoader.close()
    parentLoader.close()
  }

  test("default JDK classloader get resources") {
    val parentLoader = new URLClassLoader(fileUrlsParent, null)
    val classLoader = new URLClassLoader(fileUrlsChild, parentLoader)
    assert(classLoader.getResources("resource1").asScala.size === 2)
    assert(classLoader.getResources("resource2").asScala.size === 1)
    classLoader.close()
    parentLoader.close()
  }

  test("parent first get resources") {
    val parentLoader = new URLClassLoader(fileUrlsParent, null)
    val classLoader = new MutableURLClassLoader(fileUrlsChild, parentLoader)
    assert(classLoader.getResources("resource1").asScala.size === 2)
    assert(classLoader.getResources("resource2").asScala.size === 1)
    classLoader.close()
    parentLoader.close()
  }

  test("child first get resources") {
    val parentLoader = new URLClassLoader(fileUrlsParent, null)
    val classLoader = new ChildFirstURLClassLoader(fileUrlsChild, parentLoader)

    val res1 = classLoader.getResources("resource1").asScala.toList
    assert(res1.size === 2)
    assert(classLoader.getResources("resource2").asScala.size === 1)
    res1.map { res =>
      Utils.tryWithResource(scala.io.Source.fromURL(res))(_.mkString)
    } should contain inOrderOnly("resource1Contents-child", "resource1Contents-parent")
    classLoader.close()
    parentLoader.close()
  }


  test("driver sets context class loader in local mode") {
    // Test the case where the driver program sets a context classloader and then runs a job
    // in local mode. This is what happens when ./spark-submit is called with "local" as the
    // master.
    val original = Thread.currentThread().getContextClassLoader

    val className = "ClassForDriverTest"
    val jar = TestUtils.createJarWithClasses(Seq(className))
    val contextLoader = new URLClassLoader(Array(jar), Utils.getContextOrSparkClassLoader)
    Thread.currentThread().setContextClassLoader(contextLoader)

    val sc = new SparkContext("local", "driverLoaderTest")

    try {
      sc.makeRDD(1 to 5, 2).mapPartitions { x =>
        Utils.classForName[AnyRef](className, noSparkClassLoader = true).
          getConstructor().newInstance()
        Seq().iterator
      }.count()
    }
    catch {
      case e: SparkException if e.getMessage.contains("ClassNotFoundException") =>
        fail("Local executor could not find class", e)
      case t: Throwable => fail("Unexpected exception ", t)
    }

    sc.stop()
    Thread.currentThread().setContextClassLoader(original)
  }
}
