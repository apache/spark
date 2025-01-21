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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

class HiveUDFDynamicLoadSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  case class UDFTestInformation(
      identifier: String,
      funcName: String,
      className: String,
      fnVerifyQuery: () => Unit,
      fnCreateHiveUDFExpression: () => Expression)

  private val udfTestInfos: Seq[UDFTestInformation] = Array(
    // UDF
    // UDFExampleAdd2 is slightly modified version of UDFExampleAdd in hive/contrib,
    // which adds two integers or doubles.
    UDFTestInformation(
      "UDF",
      "udf_add2",
      "org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd2",
      () => {
        checkAnswer(sql("SELECT udf_add2(1, 2)"), Row(3) :: Nil)
      },
      () => {
        HiveSimpleUDF(
          "default.udf_add2",
          HiveFunctionWrapper("org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd2"),
          Array(
            AttributeReference("a", IntegerType, nullable = false)(),
            AttributeReference("b", IntegerType, nullable = false)()).toImmutableArraySeq)
      }),

    // GenericUDF
    // GenericUDFTrim2 is cloned version of GenericUDFTrim in hive/contrib.
    UDFTestInformation(
      "GENERIC_UDF",
      "generic_udf_trim2",
      "org.apache.hadoop.hive.contrib.udf.example.GenericUDFTrim2",
      () => {
        checkAnswer(sql("SELECT generic_udf_trim2(' hello ')"), Row("hello") :: Nil)
      },
      () => {
        HiveGenericUDF(
          "default.generic_udf_trim2",
          HiveFunctionWrapper("org.apache.hadoop.hive.contrib.udf.example.GenericUDFTrim2"),
          Array(AttributeReference("a", StringType, nullable = false)()).toImmutableArraySeq
        )
      }
    ),

    // AbstractGenericUDAFResolver
    // GenericUDAFSum2 is cloned version of GenericUDAFSum in hive/exec.
    UDFTestInformation(
      "GENERIC_UDAF",
      "generic_udaf_sum2",
      "org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum2",
      () => {
        import spark.implicits._
        val df = Seq((0: Integer) -> 0, (1: Integer) -> 1, (2: Integer) -> 2, (3: Integer) -> 3)
          .toDF("key", "value").createOrReplaceTempView("t")
        checkAnswer(sql("SELECT generic_udaf_sum2(value) FROM t GROUP BY key % 2"),
          Row(2) :: Row(4) :: Nil)
      },
      () => {
        HiveUDAFFunction(
          "default.generic_udaf_sum2",
          HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum2"),
          Array(AttributeReference("a", IntegerType, nullable = false)()).toImmutableArraySeq
        )
      }
    ),

    // UDAF
    // UDAFExampleMax2 is cloned version of UDAFExampleMax in hive/contrib.
    UDFTestInformation(
      "UDAF",
      "udaf_max2",
      "org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax2",
      () => {
        import spark.implicits._
        val df = Seq((0: Integer) -> 0, (1: Integer) -> 1, (2: Integer) -> 2, (3: Integer) -> 3)
          .toDF("key", "value").createOrReplaceTempView("t")
        checkAnswer(sql("SELECT udaf_max2(value) FROM t GROUP BY key % 2"),
          Row(2) :: Row(3) :: Nil)
      },
      () => {
        HiveUDAFFunction(
          "default.udaf_max2",
          HiveFunctionWrapper("org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax2"),
          Array(AttributeReference("a", IntegerType, nullable = false)()).toImmutableArraySeq,
          isUDAFBridgeRequired = true
        )
      }
    ),

    // GenericUDTF
    // GenericUDTFCount3 is slightly modified version of GenericUDTFCount2 in hive/contrib,
    // which emits the count for three times.
    UDFTestInformation(
      "GENERIC_UDTF",
      "udtf_count3",
      "org.apache.hadoop.hive.contrib.udtf.example.GenericUDTFCount3",
      () => {
        checkAnswer(
          sql("SELECT udtf_count3(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
          Row(3) :: Row(3) :: Row(3) :: Nil)
      },
      () => {
        HiveGenericUDTF(
          "default.udtf_count3",
          HiveFunctionWrapper("org.apache.hadoop.hive.contrib.udtf.example.GenericUDTFCount3"),
          Array.empty[Expression].toImmutableArraySeq
        )
      }
    )
  ).toImmutableArraySeq

  udfTestInfos.foreach { udfInfo =>
    // The test jars are built from below commit:
    // https://github.com/HeartSaVioR/hive/commit/12f3f036b6efd0299cd1d457c0c0a65e0fd7e5f2
    // which contain new UDF classes to be dynamically loaded and tested via Spark.

    // This jar file should not be placed to the classpath.
    val jarPath = "src/test/noclasspath/hive-test-udfs.jar"
    val jarUrl = s"file://${System.getProperty("user.dir")}/$jarPath"

    test("Spark should be able to run Hive UDF using jar regardless of " +
      s"current thread context classloader (${udfInfo.identifier}") {
      Utils.withContextClassLoader(Utils.getSparkClassLoader) {
        withUserDefinedFunction(udfInfo.funcName -> false) {
          val sparkClassLoader = Thread.currentThread().getContextClassLoader

          sql(s"CREATE FUNCTION ${udfInfo.funcName} AS '${udfInfo.className}' USING JAR '$jarUrl'")

          assert(Thread.currentThread().getContextClassLoader eq sparkClassLoader)

          // JAR will be loaded at first usage, and it will change the current thread's
          // context classloader to jar classloader in sharedState.
          // See SessionState.addJar for details.
          udfInfo.fnVerifyQuery()

          assert(Thread.currentThread().getContextClassLoader ne sparkClassLoader)
          assert(Thread.currentThread().getContextClassLoader eq
            spark.sharedState.jarClassLoader)

          val udfExpr = udfInfo.fnCreateHiveUDFExpression()
          // force initializing - this is what we do in HiveSessionCatalog
          udfExpr.dataType

          // Roll back to the original classloader and run query again. Without this line, the test
          // would pass, as thread's context classloader is changed to jar classloader. But thread
          // context classloader can be changed from others as well which would fail the query; one
          // example is spark-shell, which thread context classloader rolls back automatically. This
          // mimics the behavior of spark-shell.
          Thread.currentThread().setContextClassLoader(sparkClassLoader)

          udfInfo.fnVerifyQuery()

          val newExpr = udfExpr.makeCopy(udfExpr.productIterator.map(_.asInstanceOf[AnyRef])
            .toArray)
          newExpr.dataType
        }
      }
    }
  }
}
