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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.scalatest.{Assertions, BeforeAndAfterEach}
import org.scalatest.matchers.must.Matchers

import org.apache.spark.TestUtils
import org.apache.spark.deploy.SparkSubmitTestUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.ResetSystemProperties

// A test for growing the buffer holder to nearly 2GB. Due to the heap size limitation of the Spark
// unit tests JVM, the actually test code is running as a submit job.
class BufferHolderSparkSubmitSuite
  extends SparkSubmitTestUtils
    with Matchers
    with BeforeAndAfterEach
    with ResetSystemProperties {

  test("SPARK-22222: Buffer holder should be able to allocate memory larger than 1GB") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)

    val argsForSparkSubmit = Seq(
      "--class", BufferHolderSparkSubmitSuite.getClass.getName.stripSuffix("$"),
      "--name", "SPARK-22222",
      "--master", "local-cluster[1,1,4096]",
      "--driver-memory", "4g",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--conf", "spark.driver.extraJavaOptions=-ea",
      unusedJar.toString)
    runSparkSubmit(argsForSparkSubmit)
  }
}

object BufferHolderSparkSubmitSuite extends Assertions {

  def main(args: Array[String]): Unit = {

    val ARRAY_MAX = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

    val unsafeRow = new UnsafeRow(1000)
    val holder = new BufferHolder(unsafeRow)

    holder.reset()

    assert(intercept[IllegalArgumentException] {
      holder.grow(-1)
    }.getMessage.contains("because the size is negative"))

    // while to reuse a buffer may happen, this test checks whether the buffer can be grown
    holder.grow(ARRAY_MAX / 2)
    assert(unsafeRow.getSizeInBytes % 8 == 0)

    holder.grow(ARRAY_MAX / 2 + 7)
    assert(unsafeRow.getSizeInBytes % 8 == 0)

    holder.grow(Integer.MAX_VALUE / 2)
    assert(unsafeRow.getSizeInBytes % 8 == 0)

    holder.grow(ARRAY_MAX - holder.totalSize())
    assert(unsafeRow.getSizeInBytes % 8 == 0)

    assert(intercept[IllegalArgumentException] {
      holder.grow(ARRAY_MAX + 1 - holder.totalSize())
    }.getMessage.contains("because the size after growing"))
  }
}
