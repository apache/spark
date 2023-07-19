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
package org.apache.spark.sql.connect.artifact

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connect.common.UdfPacket
import org.apache.spark.util.{ChildFirstURLClassLoader, StubClassLoader, Utils}

class StubClassLoaderSuite extends SparkFunSuite {

  private val udfByteArray: Array[Byte] = Files.readAllBytes(Paths.get("src/test/resources/udf"))
  private val udfNoAJar = new File("src/test/resources/udf_noA.jar").toURI.toURL

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

  test("load udf") {
    // See src/test/resources/StubClassDummyUdf for how the udf and jar is created.
    val sysClassLoader = getClass.getClassLoader()
    val stubClassLoader = new RecordedStubClassLoader(null, _ => true)

    // Install artifact without class A.
    val sessionClassLoader = new ChildFirstURLClassLoader(
      Array(udfNoAJar),
      stubClassLoader,
      sysClassLoader
    )
    // Load udf with A used in the same class.
    deserializeUdf(sessionClassLoader)
    // Class A should be stubbed.
    assert(stubClassLoader.lastStubbed === "org.apache.spark.sql.connect.artifact.A")
  }

  test("unload stub class") {
    // See src/test/resources/StubClassDummyUdf for how the udf and jar is created.
    val sysClassLoader = getClass.getClassLoader()
    val stubClassLoader = new RecordedStubClassLoader(null, _ => true)

    val cl1 = new ChildFirstURLClassLoader(
      Array.empty,
      stubClassLoader,
      sysClassLoader)

    // Failed to load dummy udf
    intercept[Exception]{
      deserializeUdf(cl1)
    }
    // Successfully stubbed the missing class.
    assert(stubClassLoader.lastStubbed ===
      "org.apache.spark.sql.connect.artifact.StubClassDummyUdf")

    // Creating a new class loader will unpack the udf correctly.
    val cl2 = new ChildFirstURLClassLoader(
      Array(udfNoAJar),
      stubClassLoader, // even with the same stub class loader.
      sysClassLoader
    )
    // Should be able to load after the artifact is added
    deserializeUdf(cl2)
  }

  test("throw no such method if trying to access methods on stub class") {
    // See src/test/resources/StubClassDummyUdf for how the udf and jar is created.
    val sysClassLoader = getClass.getClassLoader()
    val stubClassLoader = new RecordedStubClassLoader(null, _ => true)

    val sessionClassLoader = new ChildFirstURLClassLoader(
      Array.empty,
      stubClassLoader,
      sysClassLoader)

    // Failed to load dummy udf
    val exception = intercept[Exception]{
      deserializeUdf(sessionClassLoader)
    }
    // Successfully stubbed the missing class.
    assert(stubClassLoader.lastStubbed ===
      "org.apache.spark.sql.connect.artifact.StubClassDummyUdf")
    // But failed to find the method on the stub class.
    val cause = exception.getCause
    assert(cause.isInstanceOf[NoSuchMethodException])
    assert(
      cause.getMessage.contains("org.apache.spark.sql.connect.artifact.StubClassDummyUdf"),
      cause.getMessage
    )
  }

  private def deserializeUdf(sessionClassLoader: ClassLoader): UdfPacket = {
    Utils.deserialize[UdfPacket](
      udfByteArray,
      sessionClassLoader
    )
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
