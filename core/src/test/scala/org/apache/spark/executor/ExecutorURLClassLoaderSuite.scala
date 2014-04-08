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

package org.apache.spark.executor

import java.io.File
import java.net.URLClassLoader

import org.scalatest.FunSuite

import org.apache.spark.TestUtils

class ExecutorURLClassLoaderSuite extends FunSuite {

  val childClassNames = List("FakeClass1", "FakeClass2")
  val parentClassNames = List("FakeClass1", "FakeClass2", "FakeClass3")
  val urls = List(TestUtils.createJarWithClassesAndValue(childClassNames, 1)).toArray
  val urls2 = List(TestUtils.createJarWithClassesAndValue(parentClassNames, 2)).toArray

  test("child first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildExecutorURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
  }

  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass1").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fall back") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildExecutorURLClassLoader(urls, parentLoader)
    val fakeClass = classLoader.loadClass("FakeClass3").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fail") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ChildExecutorURLClassLoader(urls, parentLoader)
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("FakeClassDoesNotExist").newInstance()
    }
  }


}
