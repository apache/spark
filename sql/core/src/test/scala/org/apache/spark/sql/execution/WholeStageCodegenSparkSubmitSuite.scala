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

package org.apache.spark.sql.execution

import org.scalatest.{Assertions, BeforeAndAfterEach}
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark.TestUtils
import org.apache.spark.deploy.SparkSubmitTestUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col, count, lit}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ResetSystemProperties

// Due to the need to set driver's extraJavaOptions, this test needs to use actual SparkSubmit.
class WholeStageCodegenSparkSubmitSuite extends SparkSubmitTestUtils
  with Matchers
  with BeforeAndAfterEach
  with ResetSystemProperties {

  test("Generated code on driver should not embed platform-specific constant") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)

    // HotSpot JVM specific: Set up a local cluster with the driver/executor using mismatched
    // settings of UseCompressedClassPointers JVM option.
    val argsForSparkSubmit = Seq(
      "--class", WholeStageCodegenSparkSubmitSuite.getClass.getName.stripSuffix("$"),
      "--master", "local-cluster[1,1,1024]",
      "--driver-memory", "1g",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      // SPARK-37008: The results of `Platform.BYTE_ARRAY_OFFSET` using different Java versions
      // and different args as follows table:
      // +------------------------------+--------+---------+
      // |                               |Java 8 |Java 17  |
      // +------------------------------+--------+---------+
      // |-XX:-UseCompressedOops         |  24   |   16    |
      // |-XX:+UseCompressedOops         |  16   |   16    |
      // |-XX:-UseCompressedClassPointers|  24   |   24    |
      // |-XX:+UseCompressedClassPointers|  16   |   16    |
      // +-------------------------------+-------+---------+
      // So SPARK-37008 replace `UseCompressedOops` with `UseCompressedClassPointers`.
      "--conf", "spark.driver.extraJavaOptions=-XX:-UseCompressedClassPointers",
      "--conf", "spark.executor.extraJavaOptions=-XX:+UseCompressedClassPointers",
      "--conf", "spark.sql.adaptive.enabled=false",
      unusedJar.toString)
    runSparkSubmit(argsForSparkSubmit, timeout = 3.minutes)
  }
}

object WholeStageCodegenSparkSubmitSuite extends Assertions with Logging {

  var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j2("INFO")

    spark = SparkSession.builder().getOrCreate()

    // Make sure the test is run where the driver and the executors uses different object layouts
    val driverArrayHeaderSize = Platform.BYTE_ARRAY_OFFSET
    val executorArrayHeaderSize =
      spark.sparkContext.range(0, 1).map(_ => Platform.BYTE_ARRAY_OFFSET).collect.head.toInt
    assert(driverArrayHeaderSize > executorArrayHeaderSize)

    val df = spark.range(71773).select((col("id") % lit(10)).cast(IntegerType) as "v")
      .groupBy(array(col("v"))).agg(count(col("*")))
    val plan = df.queryExecution.executedPlan
    assert(plan.exists(_.isInstanceOf[WholeStageCodegenExec]))

    val expectedAnswer =
      Row(Array(0), 7178) ::
        Row(Array(1), 7178) ::
        Row(Array(2), 7178) ::
        Row(Array(3), 7177) ::
        Row(Array(4), 7177) ::
        Row(Array(5), 7177) ::
        Row(Array(6), 7177) ::
        Row(Array(7), 7177) ::
        Row(Array(8), 7177) ::
        Row(Array(9), 7177) :: Nil
    val result = df.collect
    QueryTest.sameRows(result.toSeq, expectedAnswer) match {
      case Some(errMsg) => fail(errMsg)
      case _ =>
    }
  }
}
