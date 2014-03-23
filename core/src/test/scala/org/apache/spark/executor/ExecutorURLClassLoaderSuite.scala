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

class ExecutorURLClassLoaderSuite extends FunSuite {

  val urls = List(new File("/home/holden/repos/spark/core/src/test/resources/fake-spark-class.jar").toURI.toURL).toArray
  val urls2 = List(new File("/home/holden/repos/spark/core/src/test/resources/fake-spark-class-2.jar").toURI.toURL).toArray
  test("child first") {
    val parentLoader = new ExecutorURLClassLoader(urls2, null, false)
    val classLoader = new ExecutorURLClassLoader(urls, parentLoader, true)
    val fakeClass = classLoader.loadClass("org.apache.spark.test.FakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
  }

  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorURLClassLoader(urls, parentLoader, false)
    val fakeClass = classLoader.loadClass("org.apache.spark.test.FakeClass1").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }
}
