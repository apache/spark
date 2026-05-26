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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class TypeUtilsSuite extends SparkFunSuite with SQLHelper {

  private def typeCheckPass(types: Seq[DataType]): Unit = {
    assert(TypeUtils.checkForSameTypeInputExpr(types, "a") == TypeCheckSuccess)
  }

  private def typeCheckFail(types: Seq[DataType]): Unit = {
    assert(TypeUtils.checkForSameTypeInputExpr(types, "a")
      .isInstanceOf[DataTypeMismatch])
  }

  test("checkForSameTypeInputExpr") {
    typeCheckPass(Nil)
    typeCheckPass(StringType :: Nil)
    typeCheckPass(StringType :: StringType :: Nil)

    typeCheckFail(StringType :: IntegerType :: Nil)
    typeCheckFail(StringType :: IntegerType :: Nil)

    // Should also work on arrays. See SPARK-14990
    typeCheckPass(ArrayType(StringType, containsNull = true) ::
      ArrayType(StringType, containsNull = false) :: Nil)
  }

  test("TIMESTAMP_NANOS_TYPES_ENABLED defaults to Utils.isTesting") {
    assert(SQLConf.get.timestampNanosTypesEnabled === Utils.isTesting)
  }

  test("failUnsupportedDataType rejects timestamp nanos types when preview is disabled") {
    val ntzNanos = TimestampNTZNanosType(9)
    val ltzNanos = TimestampLTZNanosType(9)
    val nestedNtzNanos = StructType(StructField("ts", ntzNanos) :: Nil)

    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "false") {
      val conf = SQLConf.get
      val expectedParams = Map(
        "featureName" -> "Nanosecond-precision timestamp types",
        "configKey" -> "spark.sql.timestampNanosTypes.enabled",
        "configValue" -> "true")
      checkError(
        intercept[SparkException] {
          TypeUtils.failUnsupportedDataType(ntzNanos, conf)
        },
        condition = "FEATURE_NOT_ENABLED",
        parameters = expectedParams)

      checkError(
        intercept[SparkException] {
          TypeUtils.failUnsupportedDataType(ltzNanos, conf)
        },
        condition = "FEATURE_NOT_ENABLED",
        parameters = expectedParams)

      checkError(
        intercept[SparkException] {
          TypeUtils.failUnsupportedDataType(nestedNtzNanos, conf)
        },
        condition = "FEATURE_NOT_ENABLED",
        parameters = expectedParams)
    }
  }

  test("failUnsupportedDataType allows timestamp nanos types when preview is enabled") {
    val ntzNanos = TimestampNTZNanosType(9)
    val ltzNanos = TimestampLTZNanosType(9)
    val nestedLtzNanos = ArrayType(ltzNanos)

    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      TypeUtils.failUnsupportedDataType(ntzNanos, SQLConf.get)
      TypeUtils.failUnsupportedDataType(ltzNanos, SQLConf.get)
      TypeUtils.failUnsupportedDataType(nestedLtzNanos, SQLConf.get)
    }
  }

  test("failUnsupportedDataType does not reject microsecond timestamp types") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "false") {
      TypeUtils.failUnsupportedDataType(TimestampType, SQLConf.get)
      TypeUtils.failUnsupportedDataType(TimestampNTZType, SQLConf.get)
    }
  }
}
