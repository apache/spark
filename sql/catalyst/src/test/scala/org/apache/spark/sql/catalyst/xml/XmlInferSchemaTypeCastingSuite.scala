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

package org.apache.spark.sql.catalyst.xml

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.types._

/**
 * Unit tests for [[XmlInferSchema]]'s incremental primitive-type inference. `inferFrom(value,
 * typeSoFar)` refines the type inferred for a field so far, entering the `tryParse*` cascade at
 * the parser matching `typeSoFar` and reconciling the result with `compatibleType`. These tests
 * check that refining from a non-null `typeSoFar` yields the same widened type that inferring the
 * value from scratch and then merging would produce -- i.e. the incremental path is consistent
 * with the batch path. The structure mirrors `CSVInferSchemaSuite`.
 */
class XmlInferSchemaTypeCastingSuite extends SparkFunSuite with SQLHelper {

  private def newInferSchema(options: Map[String, String]): XmlInferSchema =
    new XmlInferSchema(new XmlOptions(options, "UTC", "_corrupt_record"), caseSensitive = false)

  test("String field types are inferred correctly from null types") {
    val inferSchema = newInferSchema(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"))

    assert(inferSchema.inferFrom("", NullType) == NullType)
    assert(inferSchema.inferFrom(null, NullType) == NullType)
    assert(inferSchema.inferFrom("100000000000", NullType) == LongType)
    // XML infers integral values as LongType (there is no IntegerType narrowing).
    assert(inferSchema.inferFrom("60", NullType) == LongType)
    assert(inferSchema.inferFrom("3.5", NullType) == DoubleType)
    assert(inferSchema.inferFrom("test", NullType) == StringType)
    assert(inferSchema.inferFrom("2015-08-20 15:57:00", NullType) == TimestampType)
    assert(inferSchema.inferFrom("True", NullType) == BooleanType)
    assert(inferSchema.inferFrom("FAlSE", NullType) == BooleanType)

    // A value beyond Long range is not preferred as decimal by default -> Double.
    val textValueOne = Long.MaxValue.toString + "0"
    assert(inferSchema.inferFrom(textValueOne, NullType) == DoubleType)
  }

  test("String field types are inferred correctly from other types") {
    val inferSchema = newInferSchema(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"))

    assert(inferSchema.inferFrom("1.0", LongType) == DoubleType)
    assert(inferSchema.inferFrom("test", LongType) == StringType)
    assert(inferSchema.inferFrom(null, DoubleType) == DoubleType)
    assert(inferSchema.inferFrom("test", DoubleType) == StringType)
    // A Long-so-far field seeing a timestamp string widens to String (incompatible).
    assert(inferSchema.inferFrom("2015-08-20 14:57:00", LongType) == StringType)
    assert(inferSchema.inferFrom("2015-08-20 15:57:00", DoubleType) == StringType)
    assert(inferSchema.inferFrom("True", LongType) == StringType)
    assert(inferSchema.inferFrom("FALSE", TimestampType) == StringType)

    val textValueOne = Long.MaxValue.toString + "0"
    assert(inferSchema.inferFrom(textValueOne, LongType) == DoubleType)
  }

  test("Timestamp field types are inferred correctly via custom data format") {
    var inferSchema = newInferSchema(Map("timestampFormat" -> "yyyy-mm"))
    assert(inferSchema.inferFrom("2015-08", TimestampType) == TimestampType)

    inferSchema = newInferSchema(Map("timestampFormat" -> "yyyy"))
    assert(inferSchema.inferFrom("2015", TimestampType) == TimestampType)

    val combined = Map(
      "timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
      "timestampNTZFormat" -> "yyyy-MM-dd HH:mm:ss")
    inferSchema = newInferSchema(combined)
    // Merging TimestampNTZ (so far) with a value that parses as Timestamp yields Timestamp.
    assert(inferSchema.inferFrom("2016-03-11T20:00:00", TimestampNTZType) == TimestampType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    val inferSchema = newInferSchema(Map.empty[String, String])

    assert(inferSchema.inferFrom("2015-08-20 14", LongType) == StringType)
    assert(inferSchema.inferFrom("2015-08-20 14:10", DoubleType) == StringType)
    assert(inferSchema.inferFrom("2015-08 14:49:00", LongType) == StringType)
  }

  test("Boolean field types are inferred correctly from other types") {
    val inferSchema = newInferSchema(Map.empty[String, String])

    assert(inferSchema.inferFrom("Fale", LongType) == StringType)
    assert(inferSchema.inferFrom("TRUEe", DoubleType) == StringType)
  }

  test("Empty and null values keep the type inferred so far") {
    val inferSchema = newInferSchema(Map.empty[String, String])

    // A genuinely empty/null value carries no type information, so `typeSoFar` is preserved.
    // (Unlike CSV, XML inference does not treat the `nullValue` option as null here; a value
    // equal to `nullValue` is inferred by its content, matching the pre-existing behavior.)
    assert(inferSchema.inferFrom("", NullType) == NullType)
    assert(inferSchema.inferFrom("", LongType) == LongType)
    assert(inferSchema.inferFrom(null, DoubleType) == DoubleType)
    assert(inferSchema.inferFrom(null, TimestampType) == TimestampType)
  }

  test("Refining from a wider type never narrows and is consistent with fresh inference") {
    val inferSchema = newInferSchema(Map.empty[String, String])
    // Once a field is Double, an integral value keeps it Double (does not narrow to Long).
    assert(inferSchema.inferFrom("2", DoubleType) == DoubleType)
    // Refining Double with a Double value stays Double.
    assert(inferSchema.inferFrom("1.5", DoubleType) == DoubleType)
    // An incompatible value widens all the way to String.
    assert(inferSchema.inferFrom("abc", DoubleType) == StringType)
  }

  test("date is inferred regardless of preferDate") {
    Seq("true", "false").foreach { preferDate =>
      val inferSchema = newInferSchema(Map("preferDate" -> preferDate))
      assert(inferSchema.inferFrom("2024-01-15", NullType) == DateType,
        s"expected DateType with preferDate=$preferDate")
    }
  }
}
