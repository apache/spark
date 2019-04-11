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

package org.apache.spark.sql.types

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.Cast

class DataTypeWriteCompatibilitySuite extends SparkFunSuite {
  private val atomicTypes = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType,
    DoubleType, DateType, TimestampType, StringType, BinaryType)

  private val point2 = StructType(Seq(
    StructField("x", FloatType, nullable = false),
    StructField("y", FloatType, nullable = false)))

  private val widerPoint2 = StructType(Seq(
    StructField("x", DoubleType, nullable = false),
    StructField("y", DoubleType, nullable = false)))

  private val point3 = StructType(Seq(
    StructField("x", FloatType, nullable = false),
    StructField("y", FloatType, nullable = false),
    StructField("z", FloatType)))

  private val simpleContainerTypes = Seq(
    ArrayType(LongType), ArrayType(LongType, containsNull = false), MapType(StringType, DoubleType),
    MapType(StringType, DoubleType, valueContainsNull = false), point2, point3)

  private val nestedContainerTypes = Seq(ArrayType(point2, containsNull = false),
    MapType(StringType, point3, valueContainsNull = false))

  private val allNonNullTypes = Seq(
    atomicTypes, simpleContainerTypes, nestedContainerTypes, Seq(CalendarIntervalType)).flatten

  test("Check NullType is incompatible with all other types") {
    allNonNullTypes.foreach { t =>
      assertSingleError(NullType, t, "nulls", s"Should not allow writing None to type $t") { err =>
        assert(err.contains(s"incompatible with $t"))
      }
    }
  }

  test("Check each type with itself") {
    allNonNullTypes.foreach { t =>
      assertAllowed(t, t, "t", s"Should allow writing type to itself $t")
    }
  }

  test("Check atomic types: write allowed only when casting is safe") {
    atomicTypes.foreach { w =>
      atomicTypes.foreach { r =>
        if (Cast.canUpCast(w, r)) {
          assertAllowed(w, r, "t", s"Should allow writing $w to $r because cast is safe")

        } else {
          assertSingleError(w, r, "t",
            s"Should not allow writing $w to $r because cast is not safe") { err =>
            assert(err.contains("'t'"), "Should include the field name context")
            assert(err.contains("Cannot safely cast"), "Should identify unsafe cast")
            assert(err.contains(s"$w"), "Should include write type")
            assert(err.contains(s"$r"), "Should include read type")
          }
        }
      }
    }
  }

  test("Check struct types: missing required field") {
    val missingRequiredField = StructType(Seq(StructField("x", FloatType, nullable = false)))
    assertSingleError(missingRequiredField, point2, "t",
      "Should fail because required field 'y' is missing") { err =>
      assert(err.contains("'t'"), "Should include the struct name for context")
      assert(err.contains("'y'"), "Should include the nested field name")
      assert(err.contains("missing field"), "Should call out field missing")
    }
  }

  test("Check struct types: missing starting field, matched by position") {
    val missingRequiredField = StructType(Seq(StructField("y", FloatType, nullable = false)))

    // should have 2 errors: names x and y don't match, and field y is missing
    assertNumErrors(missingRequiredField, point2, "t",
      "Should fail because field 'x' is matched to field 'y' and required field 'y' is missing", 2)
    { errs =>
      assert(errs(0).contains("'t'"), "Should include the struct name for context")
      assert(errs(0).contains("expected 'x', found 'y'"), "Should detect name mismatch")
      assert(errs(0).contains("field name does not match"), "Should identify name problem")

      assert(errs(1).contains("'t'"), "Should include the struct name for context")
      assert(errs(1).contains("'y'"), "Should include the _last_ nested fields of the read schema")
      assert(errs(1).contains("missing field"), "Should call out field missing")
    }
  }

  test("Check struct types: missing middle field, matched by position") {
    val missingMiddleField = StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("z", FloatType, nullable = false)))

    val expectedStruct = StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("y", FloatType, nullable = false),
      StructField("z", FloatType, nullable = true)))

    // types are compatible: (req int, req int) => (req int, req int, opt int)
    // but this should still fail because the names do not match.

    assertNumErrors(missingMiddleField, expectedStruct, "t",
      "Should fail because field 'y' is matched to field 'z'", 2) { errs =>
      assert(errs(0).contains("'t'"), "Should include the struct name for context")
      assert(errs(0).contains("expected 'y', found 'z'"), "Should detect name mismatch")
      assert(errs(0).contains("field name does not match"), "Should identify name problem")

      assert(errs(1).contains("'t'"), "Should include the struct name for context")
      assert(errs(1).contains("'z'"), "Should include the nested field name")
      assert(errs(1).contains("missing field"), "Should call out field missing")
    }
  }

  test("Check struct types: generic colN names are ignored") {
    val missingMiddleField = StructType(Seq(
      StructField("col1", FloatType, nullable = false),
      StructField("col2", FloatType, nullable = false)))

    val expectedStruct = StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("y", FloatType, nullable = false)))

    // types are compatible: (req int, req int) => (req int, req int)
    // names don't match, but match the naming convention used by Spark to fill in names

    assertAllowed(missingMiddleField, expectedStruct, "t",
      "Should succeed because column names are ignored")
  }

  test("Check struct types: required field is optional") {
    val requiredFieldIsOptional = StructType(Seq(
      StructField("x", FloatType),
      StructField("y", FloatType, nullable = false)))

    assertSingleError(requiredFieldIsOptional, point2, "t",
      "Should fail because required field 'x' is optional") { err =>
      assert(err.contains("'t.x'"), "Should include the nested field name context")
      assert(err.contains("Cannot write nullable values to non-null field"))
    }
  }

  test("Check struct types: data field would be dropped") {
    assertSingleError(point3, point2, "t",
      "Should fail because field 'z' would be dropped") { err =>
      assert(err.contains("'t'"), "Should include the struct name for context")
      assert(err.contains("'z'"), "Should include the extra field name")
      assert(err.contains("Cannot write extra fields"))
    }
  }

  test("Check struct types: unsafe casts are not allowed") {
    assertNumErrors(widerPoint2, point2, "t",
      "Should fail because types require unsafe casts", 2) { errs =>

      assert(errs(0).contains("'t.x'"), "Should include the nested field name context")
      assert(errs(0).contains("Cannot safely cast"))

      assert(errs(1).contains("'t.y'"), "Should include the nested field name context")
      assert(errs(1).contains("Cannot safely cast"))
    }
  }

  test("Check struct types: type promotion is allowed") {
    assertAllowed(point2, widerPoint2, "t",
      "Should allow widening float fields x and y to double")
  }

  ignore("Check struct types: missing optional field is allowed") {
    // built-in data sources do not yet support missing fields when optional
    assertAllowed(point2, point3, "t",
      "Should allow writing point (x,y) to point(x,y,z=null)")
  }

  test("Check array types: unsafe casts are not allowed") {
    val arrayOfLong = ArrayType(LongType)
    val arrayOfInt = ArrayType(IntegerType)

    assertSingleError(arrayOfLong, arrayOfInt, "arr",
      "Should not allow array of longs to array of ints") { err =>
      assert(err.contains("'arr.element'"),
        "Should identify problem with named array's element type")
      assert(err.contains("Cannot safely cast"))
    }
  }

  test("Check array types: type promotion is allowed") {
    val arrayOfLong = ArrayType(LongType)
    val arrayOfInt = ArrayType(IntegerType)
    assertAllowed(arrayOfInt, arrayOfLong, "arr",
      "Should allow array of int written to array of long column")
  }

  test("Check array types: cannot write optional to required elements") {
    val arrayOfRequired = ArrayType(LongType, containsNull = false)
    val arrayOfOptional = ArrayType(LongType)

    assertSingleError(arrayOfOptional, arrayOfRequired, "arr",
      "Should not allow array of optional elements to array of required elements") { err =>
      assert(err.contains("'arr'"), "Should include type name context")
      assert(err.contains("Cannot write nullable elements to array of non-nulls"))
    }
  }

  test("Check array types: writing required to optional elements is allowed") {
    val arrayOfRequired = ArrayType(LongType, containsNull = false)
    val arrayOfOptional = ArrayType(LongType)

    assertAllowed(arrayOfRequired, arrayOfOptional, "arr",
      "Should allow array of required elements to array of optional elements")
  }

  test("Check map value types: unsafe casts are not allowed") {
    val mapOfLong = MapType(StringType, LongType)
    val mapOfInt = MapType(StringType, IntegerType)

    assertSingleError(mapOfLong, mapOfInt, "m",
      "Should not allow map of longs to map of ints") { err =>
      assert(err.contains("'m.value'"), "Should identify problem with named map's value type")
      assert(err.contains("Cannot safely cast"))
    }
  }

  test("Check map value types: type promotion is allowed") {
    val mapOfLong = MapType(StringType, LongType)
    val mapOfInt = MapType(StringType, IntegerType)

    assertAllowed(mapOfInt, mapOfLong, "m", "Should allow map of int written to map of long column")
  }

  test("Check map value types: cannot write optional to required values") {
    val mapOfRequired = MapType(StringType, LongType, valueContainsNull = false)
    val mapOfOptional = MapType(StringType, LongType)

    assertSingleError(mapOfOptional, mapOfRequired, "m",
      "Should not allow map of optional values to map of required values") { err =>
      assert(err.contains("'m'"), "Should include type name context")
      assert(err.contains("Cannot write nullable values to map of non-nulls"))
    }
  }

  test("Check map value types: writing required to optional values is allowed") {
    val mapOfRequired = MapType(StringType, LongType, valueContainsNull = false)
    val mapOfOptional = MapType(StringType, LongType)

    assertAllowed(mapOfRequired, mapOfOptional, "m",
      "Should allow map of required elements to map of optional elements")
  }

  test("Check map key types: unsafe casts are not allowed") {
    val mapKeyLong = MapType(LongType, StringType)
    val mapKeyInt = MapType(IntegerType, StringType)

    assertSingleError(mapKeyLong, mapKeyInt, "m",
      "Should not allow map of long keys to map of int keys") { err =>
      assert(err.contains("'m.key'"), "Should identify problem with named map's key type")
      assert(err.contains("Cannot safely cast"))
    }
  }

  test("Check map key types: type promotion is allowed") {
    val mapKeyLong = MapType(LongType, StringType)
    val mapKeyInt = MapType(IntegerType, StringType)

    assertAllowed(mapKeyInt, mapKeyLong, "m",
      "Should allow map of int written to map of long column")
  }

  test("Check types with multiple errors") {
    val readType = StructType(Seq(
      StructField("a", ArrayType(DoubleType, containsNull = false)),
      StructField("arr_of_structs", ArrayType(point2, containsNull = false)),
      StructField("bad_nested_type", ArrayType(StringType)),
      StructField("m", MapType(LongType, FloatType, valueContainsNull = false)),
      StructField("map_of_structs", MapType(StringType, point3, valueContainsNull = false)),
      StructField("x", IntegerType, nullable = false),
      StructField("missing1", StringType, nullable = false),
      StructField("missing2", StringType)
    ))

    val missingMiddleField = StructType(Seq(
      StructField("x", FloatType, nullable = false),
      StructField("z", FloatType, nullable = false)))

    val writeType = StructType(Seq(
      StructField("a", ArrayType(StringType)),
      StructField("arr_of_structs", ArrayType(point3)),
      StructField("bad_nested_type", point3),
      StructField("m", MapType(DoubleType, DoubleType)),
      StructField("map_of_structs", MapType(StringType, missingMiddleField)),
      StructField("y", LongType)
    ))

    assertNumErrors(writeType, readType, "top", "Should catch 14 errors", 14) { errs =>
      assert(errs(0).contains("'top.a.element'"), "Should identify bad type")
      assert(errs(0).contains("Cannot safely cast"))
      assert(errs(0).contains("StringType to DoubleType"))

      assert(errs(1).contains("'top.a'"), "Should identify bad type")
      assert(errs(1).contains("Cannot write nullable elements to array of non-nulls"))

      assert(errs(2).contains("'top.arr_of_structs.element'"), "Should identify bad type")
      assert(errs(2).contains("'z'"), "Should identify bad field")
      assert(errs(2).contains("Cannot write extra fields to struct"))

      assert(errs(3).contains("'top.arr_of_structs'"), "Should identify bad type")
      assert(errs(3).contains("Cannot write nullable elements to array of non-nulls"))

      assert(errs(4).contains("'top.bad_nested_type'"), "Should identify bad type")
      assert(errs(4).contains("is incompatible with"))

      assert(errs(5).contains("'top.m.key'"), "Should identify bad type")
      assert(errs(5).contains("Cannot safely cast"))
      assert(errs(5).contains("DoubleType to LongType"))

      assert(errs(6).contains("'top.m.value'"), "Should identify bad type")
      assert(errs(6).contains("Cannot safely cast"))
      assert(errs(6).contains("DoubleType to FloatType"))

      assert(errs(7).contains("'top.m'"), "Should identify bad type")
      assert(errs(7).contains("Cannot write nullable values to map of non-nulls"))

      assert(errs(8).contains("'top.map_of_structs.value'"), "Should identify bad type")
      assert(errs(8).contains("expected 'y', found 'z'"), "Should detect name mismatch")
      assert(errs(8).contains("field name does not match"), "Should identify name problem")

      assert(errs(9).contains("'top.map_of_structs.value'"), "Should identify bad type")
      assert(errs(9).contains("'z'"), "Should identify missing field")
      assert(errs(9).contains("missing fields"), "Should detect missing field")

      assert(errs(10).contains("'top.map_of_structs'"), "Should identify bad type")
      assert(errs(10).contains("Cannot write nullable values to map of non-nulls"))

      assert(errs(11).contains("'top.x'"), "Should identify bad type")
      assert(errs(11).contains("Cannot safely cast"))
      assert(errs(11).contains("LongType to IntegerType"))

      assert(errs(12).contains("'top'"), "Should identify bad type")
      assert(errs(12).contains("expected 'x', found 'y'"), "Should detect name mismatch")
      assert(errs(12).contains("field name does not match"), "Should identify name problem")

      assert(errs(13).contains("'top'"), "Should identify bad type")
      assert(errs(13).contains("'missing1'"), "Should identify missing field")
      assert(errs(13).contains("missing fields"), "Should detect missing field")
    }
  }

  // Helper functions

  def assertAllowed(writeType: DataType, readType: DataType, name: String, desc: String): Unit = {
    assert(
      DataType.canWrite(writeType, readType, analysis.caseSensitiveResolution, name,
        errMsg => fail(s"Should not produce errors but was called with: $errMsg")), desc)
  }

  def assertSingleError(
      writeType: DataType,
      readType: DataType,
      name: String,
      desc: String)
      (errFunc: String => Unit): Unit = {
    assertNumErrors(writeType, readType, name, desc, 1) { errs =>
      errFunc(errs.head)
    }
  }

  def assertNumErrors(
      writeType: DataType,
      readType: DataType,
      name: String,
      desc: String,
      numErrs: Int)
      (errFunc: Seq[String] => Unit): Unit = {
    val errs = new mutable.ArrayBuffer[String]()
    assert(
      DataType.canWrite(writeType, readType, analysis.caseSensitiveResolution, name,
        errMsg => errs += errMsg) === false, desc)
    assert(errs.size === numErrs, s"Should produce $numErrs error messages")
    errFunc(errs)
  }
}
