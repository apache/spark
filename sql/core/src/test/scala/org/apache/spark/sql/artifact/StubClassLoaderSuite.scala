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
package org.apache.spark.sql.artifact

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.{ChildFirstURLClassLoader, StubClassLoader}

class StubClassLoaderSuite extends SparkFunSuite {

  // TODO: Modify JAR to remove references to connect.
  // See connector/client/jvm/src/test/resources/StubClassDummyUdf for how the UDFs and jars are
  // created.
  private val udfNoAJar = new File(
    "src/test/resources/artifact-tests/udf_noA.jar").toURI.toURL
  private val classDummyUdf = "org.apache.spark.sql.connect.client.StubClassDummyUdf"
  private val classA = "org.apache.spark.sql.connect.client.A"

  test("find class with stub class") {
    val cl = new RecordedStubClassLoader(getClass().getClassLoader(), _ => true)
    val cls = cl.findClass("my.name.HelloWorld")
    assert(cls.getName === "my.name.HelloWorld")
    assert(cl.lastStubbed === "my.name.HelloWorld")
  }

  test("class for name with stub class") {
    val cl = new RecordedStubClassLoader(getClass().getClassLoader(), _ => true)
    // scalastyle:off classforname
    val cls = Class.forName("my.name.HelloWorld", false, cl)
    // scalastyle:on classforname
    assert(cls.getName === "my.name.HelloWorld")
    assert(cl.lastStubbed === "my.name.HelloWorld")
  }

  test("filter class to stub") {
    val list = "my.name" :: Nil
    val cl = StubClassLoader(getClass().getClassLoader(), list)
    val cls = cl.findClass("my.name.HelloWorld")
    assert(cls.getName === "my.name.HelloWorld")

    intercept[ClassNotFoundException] {
      cl.findClass("name.my.GoodDay")
    }
  }

  test("call stub class default constructor") {
    val cl = new RecordedStubClassLoader(getClass().getClassLoader(), _ => true)
    // scalastyle:off classforname
    val cls = Class.forName("my.name.HelloWorld", false, cl)
    // scalastyle:on classforname
    assert(cl.lastStubbed === "my.name.HelloWorld")
    val error = intercept[java.lang.reflect.InvocationTargetException] {
      cls.getDeclaredConstructor().newInstance()
    }
    assert(
      error.getCause != null && error.getCause.getMessage.contains(
        "Fail to initiate the class my.name.HelloWorld because it is stubbed"),
      error)
  }

  test("stub missing class") {
    val sysClassLoader = getClass.getClassLoader()
    val stubClassLoader = new RecordedStubClassLoader(null, _ => true)

    // Install artifact without class A.
    val sessionClassLoader =
      new ChildFirstURLClassLoader(Array(udfNoAJar), stubClassLoader, sysClassLoader)
    // Load udf with A used in the same class.
    loadDummyUdf(sessionClassLoader)
    // Class A should be stubbed.
    assert(stubClassLoader.lastStubbed === classA)
  }

  test("unload stub class") {
    val sysClassLoader = getClass.getClassLoader()
    val stubClassLoader = new RecordedStubClassLoader(null, _ => true)

    val cl1 = new ChildFirstURLClassLoader(Array.empty, stubClassLoader, sysClassLoader)

    // Failed to load DummyUdf
    intercept[Exception] {
      loadDummyUdf(cl1)
    }
    // Successfully stubbed the missing class.
    assert(stubClassLoader.lastStubbed === classDummyUdf)

    // Creating a new class loader will unpack the udf correctly.
    val cl2 = new ChildFirstURLClassLoader(
      Array(udfNoAJar),
      stubClassLoader, // even with the same stub class loader.
      sysClassLoader)
    // Should be able to load after the artifact is added
    loadDummyUdf(cl2)
  }

  test("throw no such method if trying to access methods on stub class") {
    val sysClassLoader = getClass.getClassLoader()
    val stubClassLoader = new RecordedStubClassLoader(null, _ => true)

    val sessionClassLoader =
      new ChildFirstURLClassLoader(Array.empty, stubClassLoader, sysClassLoader)

    // Failed to load DummyUdf because of missing methods
    assert(intercept[NoSuchMethodException] {
      loadDummyUdf(sessionClassLoader)
    }.getMessage.contains(classDummyUdf))
    // Successfully stubbed the missing class.
    assert(stubClassLoader.lastStubbed === classDummyUdf)
  }

  private def loadDummyUdf(sessionClassLoader: ClassLoader): Unit = {
    // Load DummyUdf and call a method on it.
    // scalastyle:off classforname
    val cls = Class.forName(classDummyUdf, false, sessionClassLoader)
    // scalastyle:on classforname
    cls.getDeclaredMethod("dummy")

    // Load class A used inside DummyUdf
    // scalastyle:off classforname
    Class.forName(classA, false, sessionClassLoader)
    // scalastyle:on classforname
  }
}

class RecordedStubClassLoader(parent: ClassLoader, shouldStub: String => Boolean)
    extends StubClassLoader(parent, shouldStub) {
  var lastStubbed: String = _

  override def findClass(name: String): Class[_] = {
    if (shouldStub(name)) {
      lastStubbed = name
    }
    super.findClass(name)
  }
}
