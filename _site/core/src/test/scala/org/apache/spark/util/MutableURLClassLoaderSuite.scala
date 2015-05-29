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

import org.scalatest.FunSuite

import org.apache.spark.{SparkContext, SparkException, TestUtils}

class MutableURLClassLoaderSuite extends FunSuite {

  val urls2 = List(TestUtils.createJarWithClasses(
      classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
      toStringValue = "2")).toArray
  val urls = List(TestUtils.createJarWithClasses(
      classNames = Seq("FakeClass1"),
      classNamesWithBase = Seq(("FakeClass2", "FakeClass3")), // FakeClass3 is in parent
      toStringValue = "1",
      classpathUrls = urls2)).toArray

  test("child first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildFirstURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    val fakeClass2 = classLoader.loadClass("FakeClass2").newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
  }

  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new MutableURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass1").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
    val fakeClass2 = classLoader.loadClass("FakeClass1").newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
  }

  test("child first can fall back") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildFirstURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass3").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fail") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildFirstURLClassLoader(urls, parentLoader)
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("FakeClassDoesNotExist").newInstance()
    }
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
        val loader = Thread.currentThread().getContextClassLoader
        Class.forName(className, true, loader).newInstance()
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
