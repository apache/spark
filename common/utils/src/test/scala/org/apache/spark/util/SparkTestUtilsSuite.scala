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

import java.util.concurrent.{CountDownLatch, Executors}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class SparkTestUtilsSuite extends AnyFunSuite with SparkTestUtils { // scalastyle:ignore funsuite

  test("SPARK-57081: createCompiledClass with spaces in classpath") {
    val dir = SparkFileUtils.createTempDir(namePrefix = "path with spaces")
    val sourceFile = new JavaSourceFromString("Hello", "public class Hello {}")
    val result = createCompiledClass("Hello", dir, sourceFile, Seq(dir.toURI.toURL))
    assert(result.exists(), s"Compiled class file should exist at ${result.getPath}")
  }

  test("createCompiledClass supports concurrent compilation of the same class name") {
    val executor = Executors.newFixedThreadPool(8)
    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutorService(executor)
    val start = new CountDownLatch(1)
    try {
      val compiledClasses = (1 to 8).map { _ =>
        Future {
          val dir = SparkFileUtils.createTempDir()
          val sourceFile = new JavaSourceFromString("Hello", "public class Hello {}")
          start.await()
          createCompiledClass("Hello", dir, sourceFile, Seq.empty)
        }
      }
      start.countDown()

      assert(Await.result(Future.sequence(compiledClasses), 30.seconds).forall(_.exists()))
    } finally {
      executor.shutdownNow()
    }
  }
}
