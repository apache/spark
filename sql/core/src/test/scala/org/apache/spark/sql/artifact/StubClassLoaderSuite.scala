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
import org.apache.spark.util.{ChildFirstURLClassLoader, SparkTestUtils, StubClassLoader, Utils}

class StubClassLoaderSuite extends SparkFunSuite {

  // Dynamically compiled with Helper class excluded from the JAR.
  private lazy val dummyNoHelperJar = {
    val source =
      """package org.apache.spark.sql.artifact.test
        |class Dummy { val dummy = (x: Int) => Helper(x) }
        |case class Helper(x: Int) { def get: Int = x + 5 }
        |""".stripMargin
    val srcFile = File.createTempFile("StubClassDummy", ".scala", Utils.createTempDir())
    val pw = new java.io.PrintWriter(srcFile)
    try { pw.write(source) } finally { pw.close() }
    val jarFile = new File(Utils.createTempDir(), "dummy_noHelper.jar")
    val cp = System.getProperty("java.class.path")
      .split(File.pathSeparator).map(p => new File(p).toURI.toURL).toSeq
    SparkTestUtils.createJarWithScalaSources(
      Seq(srcFile), jarFile, cp,
      excludeClassPrefixes = Seq("Helper"))
  }
  private val classDummy = "org.apache.spark.sql.artifact.test.Dummy"
  private val classHelper = "org.apache.spark.sql.artifact.test.Helper"

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

    // Install artifact without Helper class.
    val sessionClassLoader =
      new ChildFirstURLClassLoader(Array(dummyNoHelperJar), stubClassLoader, sysClassLoader)
    // Load Dummy which references Helper.
    loadDummy(sessionClassLoader)
    // Helper should be stubbed.
    assert(stubClassLoader.lastStubbed === classHelper)
  }

  test("unload stub class") {
    val sysClassLoader = getClass.getClassLoader()
    val stubClassLoader = new RecordedStubClassLoader(null, _ => true)

    val cl1 = new ChildFirstURLClassLoader(Array.empty, stubClassLoader, sysClassLoader)

    // Failed to load Dummy
    intercept[Exception] {
      loadDummy(cl1)
    }
    // Successfully stubbed the missing class.
    assert(stubClassLoader.lastStubbed === classDummy)

    // Creating a new class loader will load the class correctly.
    val cl2 = new ChildFirstURLClassLoader(
      Array(dummyNoHelperJar),
      stubClassLoader, // even with the same stub class loader.
      sysClassLoader)
    // Should be able to load after the artifact is added
    loadDummy(cl2)
  }

  test("throw no such method if trying to access methods on stub class") {
    val sysClassLoader = getClass.getClassLoader()
    val stubClassLoader = new RecordedStubClassLoader(null, _ => true)

    val sessionClassLoader =
      new ChildFirstURLClassLoader(Array.empty, stubClassLoader, sysClassLoader)

    // Failed to load Dummy because of missing methods
    assert(intercept[NoSuchMethodException] {
      loadDummy(sessionClassLoader)
    }.getMessage.contains(classDummy))
    // Successfully stubbed the missing class.
    assert(stubClassLoader.lastStubbed === classDummy)
  }

  private def loadDummy(sessionClassLoader: ClassLoader): Unit = {
    // Load Dummy and call a method on it.
    // scalastyle:off classforname
    val cls = Class.forName(classDummy, false, sessionClassLoader)
    // scalastyle:on classforname
    cls.getDeclaredMethod("dummy")

    // Load Helper class used inside Dummy
    // scalastyle:off classforname
    Class.forName(classHelper, false, sessionClassLoader)
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
