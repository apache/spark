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
    assert(inferSchema.inferFrom("", NullType) == NullType)
    assert(inferSchema.inferFrom("", LongType) == LongType)
    assert(inferSchema.inferFrom(null, DoubleType) == DoubleType)
    assert(inferSchema.inferFrom(null, TimestampType) == TimestampType)
  }

  test("A value matching the nullValue option keeps the type inferred so far") {
    // A token equal to the `nullValue` option is read as null by the parser, so inference must
    // ignore it and preserve `typeSoFar` -- matching `CSVInferSchema.inferField`. Otherwise a
    // `nullValue` token would be inferred by its string content and (incorrectly) widen the field.
    val inferSchema = newInferSchema(Map("nullValue" -> "NA"))
    assert(inferSchema.inferFrom("NA", NullType) == NullType)
    assert(inferSchema.inferFrom("NA", LongType) == LongType)
    assert(inferSchema.inferFrom("NA", TimestampType) == TimestampType)
    // A non-null value is still inferred by content as usual.
    assert(inferSchema.inferFrom("5", NullType) == LongType)

    // The nullValue token itself, when it happens to look like another type, is still treated as
    // null rather than inferred as that type.
    val inferSchemaNumericNull = newInferSchema(Map("nullValue" -> "0"))
    assert(inferSchemaNumericNull.inferFrom("0", NullType) == NullType)
    assert(inferSchemaNumericNull.inferFrom("1", NullType) == LongType)
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

  test("Refining a numeric type-so-far matches fresh inference (SPARK-57802)") {
    // Refining a numeric `typeSoFar` re-enters the cascade at `tryParseLong`, so an integer value
    // infers as `Long` (not a narrow `Decimal`) exactly as from-scratch inference would, and the
    // merge with `typeSoFar` yields the same type as the legacy path. Otherwise, under
    // prefersDecimal, a `Decimal`-so-far field seeing "5" would infer `Decimal(1,0)` and merge to
    // a different (order-dependent) precision than the legacy `Long`-then-merge result.
    val prefersDecimal = newInferSchema(Map("prefersDecimal" -> "true"))
    // "5" fresh -> Long; compatibleType(Decimal(5,2), Long) -> Decimal(22,2).
    assert(prefersDecimal.inferFrom("5", DecimalType(5, 2)) == DecimalType(22, 2))
    assert(prefersDecimal.inferFrom("5", DoubleType) == DoubleType)
    // A fractional value still refines through the decimal/double parsers as usual: "3.5" infers
    // as Decimal(2,1) and merges with Decimal(5,2) to Decimal(5,2).
    assert(prefersDecimal.inferFrom("3.5", DecimalType(5, 2)) == DecimalType(5, 2))
  }

  test("Refining a temporal type-so-far matches fresh inference (SPARK-57802)") {
    // Refining a temporal `typeSoFar` re-enters the cascade at the top of the temporal sub-cascade
    // (`tryParseTime`, flowing Time -> Date -> TimestampNTZ -> Timestamp), so a date-only value
    // infers as `Date` exactly as from-scratch inference would, and the merge with `typeSoFar`
    // widens through `findWiderDateTimeType` to the same type as the legacy path. Otherwise, with a
    // time-requiring `timestampFormat`, a `Timestamp`-so-far field seeing a date-only value would
    // fail `tryParseTimestamp`, fall through to `String`, and merge to `String` -- an
    // order-dependent divergence from the legacy `Date`-then-merge result.
    val inferSchema = newInferSchema(Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss"))
    // "2024-01-15" fresh -> Date; compatibleType(Timestamp, Date) -> Timestamp (DATE adopts the
    // LTZ family of the other side).
    assert(inferSchema.inferFrom("2024-01-15", TimestampType) == TimestampType)
    // Same for a TimestampNTZ-so-far field: DATE adopts the NTZ family, so the merge stays NTZ.
    assert(inferSchema.inferFrom("2024-01-15", TimestampNTZType) == TimestampNTZType)
    // A Date-so-far field seeing a timestamp value widens to Timestamp.
    assert(inferSchema.inferFrom("2024-01-15T10:00:00", DateType) == TimestampType)
  }

  test("preferDate gates date inference (consistent with CSV)") {
    // preferDate controls whether date inference is attempted, matching CSVInferSchema: when true
    // a bare date infers as DateType; when false date inference is skipped and the value falls
    // through to timestamp inference.
    assert(newInferSchema(Map("preferDate" -> "true"))
      .inferFrom("2024-01-15", NullType) == DateType)
    assert(newInferSchema(Map("preferDate" -> "false"))
      .inferFrom("2024-01-15", NullType) == TimestampType)
  }

  test("preferDate gates the incremental temporal re-entry (tryParseTime)") {
    // Refining a temporal `typeSoFar` re-enters the cascade at `tryParseTime`, which must honor
    // `preferDate` just like the fresh path (`tryParseDouble`). With a `Date`-so-far field seeing
    // another date-only value: preferDate=true re-infers `Date` and the merge stays `Date`;
    // preferDate=false skips date inference so the value falls through to `Timestamp`, and the
    // merge widens `Date` to `Timestamp`. This guards the `tryParseTime` branch, which the
    // incremental-vs-legacy parity test does not cover (it runs only at the default
    // preferDate=true).
    assert(newInferSchema(Map("preferDate" -> "true"))
      .inferFrom("2024-01-15", DateType) == DateType)
    assert(newInferSchema(Map("preferDate" -> "false"))
      .inferFrom("2024-01-15", DateType) == TimestampType)
  }

  test("TIME inference precedes the preferDate gate") {
    // TIME is tried ahead of date/timestamp in both `tryParseDouble` (fresh path) and
    // `tryParseTime` (incremental temporal re-entry), so a TIME-shaped value infers as `TimeType`
    // regardless of `preferDate`. This guards against a refactor that would move the TIME guard
    // inside the `preferDate` branch and silently drop TIME inference when preferDate=false.
    val time = TimeType(TimeType.DEFAULT_PRECISION)
    Seq("true", "false").foreach { preferDate =>
      val inferSchema = newInferSchema(Map("preferDate" -> preferDate))
      // Fresh path (typeSoFar == NullType, enters via tryParseDouble).
      assert(inferSchema.inferFrom("10:00:00", NullType) == time,
        s"expected TimeType with preferDate=$preferDate")
      // Incremental temporal re-entry (typeSoFar == TimeType, enters via tryParseTime).
      assert(inferSchema.inferFrom("10:00:00", time) == time,
        s"expected TimeType with preferDate=$preferDate")
    }
  }
}
