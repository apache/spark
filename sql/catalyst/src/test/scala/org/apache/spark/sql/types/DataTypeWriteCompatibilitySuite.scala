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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLType
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy

class StrictDataTypeWriteCompatibilitySuite extends DataTypeWriteCompatibilityBaseSuite {
  override def storeAssignmentPolicy: SQLConf.StoreAssignmentPolicy.Value =
    StoreAssignmentPolicy.STRICT

  override def canCast: (DataType, DataType) => Boolean = Cast.canUpCast

  test("Check struct types: unsafe casts are not allowed") {
    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", widerPoint2, point2, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`t`.`x`",
        "srcType" -> "\"DOUBLE\"",
        "targetType" -> "\"FLOAT\"")
    )
  }

  test("Check array types: unsafe casts are not allowed") {
    val arrayOfLong = ArrayType(LongType)
    val arrayOfInt = ArrayType(IntegerType)

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", arrayOfLong, arrayOfInt, true,
          analysis.caseSensitiveResolution, "arr", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`arr`.`element`",
        "srcType" -> "\"BIGINT\"",
        "targetType" -> "\"INT\"")
    )
  }

  test("Check map value types: casting Long to Integer is not allowed") {
    val mapOfLong = MapType(StringType, LongType)
    val mapOfInt = MapType(StringType, IntegerType)

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", mapOfLong, mapOfInt, true,
          analysis.caseSensitiveResolution, "m", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`m`.`value`",
        "srcType" -> "\"BIGINT\"",
        "targetType" -> "\"INT\"")
    )
  }

  test("Check map key types: unsafe casts are not allowed") {
    val mapKeyLong = MapType(LongType, StringType)
    val mapKeyInt = MapType(IntegerType, StringType)

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", mapKeyLong, mapKeyInt, true,
          analysis.caseSensitiveResolution, "m", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`m`.`key`",
        "srcType" -> "\"BIGINT\"",
        "targetType" -> "\"INT\"")
    )
  }

  test("Check NullType is incompatible with all other types") {
    allNonNullTypes.foreach { t =>
      val errs = new mutable.ArrayBuffer[String]()
      checkError(
        exception = intercept[AnalysisException] (
          DataTypeUtils.canWrite("", NullType, t, true,
            analysis.caseSensitiveResolution, "nulls", storeAssignmentPolicy,
            errMsg => errs += errMsg)
        ),
        condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
        parameters = Map(
          "tableName" -> "``",
          "colName" -> "`nulls`",
          "srcType" -> "\"VOID\"",
          "targetType" -> toSQLType(t.catalogString))
      )
    }
  }
}

class ANSIDataTypeWriteCompatibilitySuite extends DataTypeWriteCompatibilityBaseSuite {
  override protected def storeAssignmentPolicy: SQLConf.StoreAssignmentPolicy.Value =
    StoreAssignmentPolicy.ANSI

  override def canCast: (DataType, DataType) => Boolean = Cast.canANSIStoreAssign

  test("Check map value types: unsafe casts are not allowed") {
    val mapOfString = MapType(StringType, StringType)
    val mapOfInt = MapType(StringType, IntegerType)

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", mapOfString, mapOfInt, true,
          analysis.caseSensitiveResolution, "m", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`m`.`value`",
        "srcType" -> "\"STRING\"",
        "targetType" -> "\"INT\"")
    )
  }

  private val stringPoint2 = StructType(Seq(
    StructField("x", StringType, nullable = false),
    StructField("y", StringType, nullable = false)))

  test("Check struct types: unsafe casts are not allowed") {
    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", stringPoint2, point2, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`t`.`x`",
        "srcType" -> "\"STRING\"",
        "targetType" -> "\"FLOAT\"")
    )
  }

  test("Check array types: unsafe casts are not allowed") {
    val arrayOfString = ArrayType(StringType)
    val arrayOfInt = ArrayType(IntegerType)

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", arrayOfString, arrayOfInt, true,
          analysis.caseSensitiveResolution, "arr", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`arr`.`element`",
        "srcType" -> "\"STRING\"",
        "targetType" -> "\"INT\"")
    )
  }

  test("Check map key types: unsafe casts are not allowed") {
    val mapKeyString = MapType(StringType, StringType)
    val mapKeyInt = MapType(IntegerType, StringType)

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", mapKeyString, mapKeyInt, true,
          analysis.caseSensitiveResolution, "arr", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`arr`.`key`",
        "srcType" -> "\"STRING\"",
        "targetType" -> "\"INT\"")
    )
  }

  test("Conversions between timestamp and long are not allowed") {
    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", LongType, TimestampType, true,
          analysis.caseSensitiveResolution, "longToTimestamp", storeAssignmentPolicy,
          errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`longToTimestamp`",
        "srcType" -> "\"BIGINT\"",
        "targetType" -> "\"TIMESTAMP\"")
    )
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", TimestampType, LongType, true,
          analysis.caseSensitiveResolution, "timestampToLong", storeAssignmentPolicy,
          errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`timestampToLong`",
        "srcType" -> "\"TIMESTAMP\"",
        "targetType" -> "\"BIGINT\"")
    )
  }

  test("SPARK-37707: Check datetime types compatible with each other") {
    val dateTimeTypes = Seq(DateType, TimestampType, TimestampNTZType)
    dateTimeTypes.foreach { t1 =>
      dateTimeTypes.foreach { t2 =>
        assertAllowed(t1, t2, "date time types", s"Should allow writing $t1 to type $t2")
      }
    }
  }

  test("Check NullType is compatible with all other types") {
    allNonNullTypes.foreach { t =>
      assertAllowed(NullType, t, "nulls", s"Should allow writing None to type $t")
    }
  }
}

abstract class DataTypeWriteCompatibilityBaseSuite extends SparkFunSuite {
  protected def storeAssignmentPolicy: StoreAssignmentPolicy.Value

  protected def canCast: (DataType, DataType) => Boolean

  protected val atomicTypes = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DateType, TimestampType, StringType, BinaryType)

  protected val point2 = StructType(Seq(
    StructField("x", FloatType, nullable = false),
    StructField("y", FloatType, nullable = false)))

  protected val widerPoint2 = StructType(Seq(
    StructField("x", DoubleType, nullable = false),
    StructField("y", DoubleType, nullable = false)))

  protected val point3 = StructType(Seq(
    StructField("x", FloatType, nullable = false),
    StructField("y", FloatType, nullable = false),
    StructField("z", FloatType)))

  private val simpleContainerTypes = Seq(
    ArrayType(LongType), ArrayType(LongType, containsNull = false), MapType(StringType, DoubleType),
    MapType(StringType, DoubleType, valueContainsNull = false), point2, point3)

  private val nestedContainerTypes = Seq(ArrayType(point2, containsNull = false),
    MapType(StringType, point3, valueContainsNull = false))

  protected val allNonNullTypes = Seq(
    atomicTypes, simpleContainerTypes, nestedContainerTypes, Seq(CalendarIntervalType)).flatten

  test("Check each type with itself") {
    allNonNullTypes.foreach { t =>
      assertAllowed(t, t, "t", s"Should allow writing type to itself $t")
    }
  }

  test("Check atomic types: write allowed only when casting is safe") {
    atomicTypes.foreach { w =>
      atomicTypes.foreach { r =>
        if (canCast(w, r)) {
          assertAllowed(w, r, "t", s"Should allow writing $w to $r because cast is safe")

        } else {
          val errs = new mutable.ArrayBuffer[String]()
          checkError(
            exception = intercept[AnalysisException] (
              DataTypeUtils.canWrite("", w, r, true, analysis.caseSensitiveResolution, "t",
                storeAssignmentPolicy, errMsg => errs += errMsg)
            ),
            condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
            parameters = Map(
              "tableName" -> "``",
              "colName" -> "`t`",
              "srcType" -> toSQLType(w),
              "targetType" -> toSQLType(r)
            ),
            matchPVals = true
          )
        }
      }
    }
  }

  test("Check struct types: missing required field") {
    val missingRequiredField = StructType(Seq(StructField("x", FloatType, nullable = false)))
    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", missingRequiredField, point2, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS",
      parameters = Map("tableName" -> "``", "colName" -> "`t`", "missingFields" -> "`y`")
    )
  }

  test("Check struct types: missing starting field, matched by position") {
    val missingRequiredField = StructType(Seq(StructField("y", FloatType, nullable = false)))
    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", missingRequiredField, point2, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.UNEXPECTED_COLUMN_NAME",
      parameters = Map(
        "expected" -> "`x`",
        "found" -> "`y`",
        "tableName" -> "``",
        "colName" -> "`t`",
        "order" -> "0")
    )
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
    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", missingMiddleField, expectedStruct, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.UNEXPECTED_COLUMN_NAME",
      parameters = Map(
        "expected" -> "`y`",
        "found" -> "`z`",
        "tableName" -> "``",
        "colName" -> "`t`",
        "order" -> "1")
    )
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

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", requiredFieldIsOptional, point2, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.NULLABLE_COLUMN",
      parameters = Map("tableName" -> "``", "colName" -> "`t`.`x`")
    )
  }

  test("Check struct types: data field would be dropped") {
    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", point3, point2, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS",
      parameters = Map("tableName" -> "``", "colName" -> "`t`", "extraFields" -> "`z`")
    )
  }

  test("Check struct types: type promotion is allowed") {
    assertAllowed(point2, widerPoint2, "t",
      "Should allow widening float fields x and y to double")
  }

  test("Check struct type: ignore field name mismatch with byPosition mode") {
    val nameMismatchFields = StructType(Seq(
      StructField("a", FloatType, nullable = false),
      StructField("b", FloatType, nullable = false)))
    assertAllowed(nameMismatchFields, point2, "t",
      "Should allow field name mismatch with byPosition mode", false)
  }

  ignore("Check struct types: missing optional field is allowed") {
    // built-in data sources do not yet support missing fields when optional
    assertAllowed(point2, point3, "t",
      "Should allow writing point (x,y) to point(x,y,z=null)")
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

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", arrayOfOptional, arrayOfRequired, true,
          analysis.caseSensitiveResolution, "arr", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.NULLABLE_ARRAY_ELEMENTS",
      parameters = Map("tableName" -> "``", "colName" -> "`arr`")
    )
  }

  test("Check array types: writing required to optional elements is allowed") {
    val arrayOfRequired = ArrayType(LongType, containsNull = false)
    val arrayOfOptional = ArrayType(LongType)

    assertAllowed(arrayOfRequired, arrayOfOptional, "arr",
      "Should allow array of required elements to array of optional elements")
  }

  test("Check map value types: type promotion is allowed") {
    val mapOfLong = MapType(StringType, LongType)
    val mapOfInt = MapType(StringType, IntegerType)

    assertAllowed(mapOfInt, mapOfLong, "m", "Should allow map of int written to map of long column")
  }

  test("Check map value types: cannot write optional to required values") {
    val mapOfRequired = MapType(StringType, LongType, valueContainsNull = false)
    val mapOfOptional = MapType(StringType, LongType)

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", mapOfOptional, mapOfRequired, true,
          analysis.caseSensitiveResolution, "m", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.NULLABLE_MAP_VALUES",
      parameters = Map("tableName" -> "``", "colName" -> "`m`")
    )
  }

  test("Check map value types: writing required to optional values is allowed") {
    val mapOfRequired = MapType(StringType, LongType, valueContainsNull = false)
    val mapOfOptional = MapType(StringType, LongType)

    assertAllowed(mapOfRequired, mapOfOptional, "m",
      "Should allow map of required elements to map of optional elements")
  }

  test("Check map key types: type promotion is allowed") {
    val mapKeyLong = MapType(LongType, StringType)
    val mapKeyInt = MapType(IntegerType, StringType)

    assertAllowed(mapKeyInt, mapKeyLong, "m",
      "Should allow map of int written to map of long column")
  }

  test("SPARK-47528: Check udt: underlying sql type is same") {
    val udtType = new UserDefinedType[Any] {
      override def sqlType: DataType = StructType(Seq(
        StructField("col1", FloatType, nullable = false),
        StructField("col2", FloatType, nullable = false)))

      override def userClass: java.lang.Class[Any] = null

      override def serialize(obj: Any): Any = null

      override def deserialize(datum: Any): Any = null
    }

    val sqlType = StructType(Seq(
      StructField("col1", FloatType, nullable = false),
      StructField("col2", FloatType, nullable = false)))

    assertAllowed(udtType, sqlType, "m",
      "Should allow udt with same sqlType written to struct column")

    assertAllowed(sqlType, udtType, "m",
      "Should allow udt with same sqlType written to struct column")
  }

  test("SPARK-47528: Check udt: underlying sql type is same but different nullability") {
    val udtType = new UserDefinedType[Any] {
      override def sqlType: DataType = StructType(Seq(
        StructField("col1", FloatType, nullable = false),
        StructField("col2", FloatType, nullable = false)))

      override def userClass: java.lang.Class[Any] = null

      override def serialize(obj: Any): Any = null

      override def deserialize(datum: Any): Any = null
    }

    val sqlType = StructType(Seq(
      StructField("col1", FloatType, nullable = false),
      StructField("col2", FloatType, nullable = true)))

    assertAllowed(udtType, sqlType, "m",
      "Should allow udt with same sqlType written to struct column")

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", sqlType, udtType, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.NULLABLE_COLUMN",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`t`.`col2`"
      )
    )
  }

  test("SPARK-47528: Check udt: write underlying sql type is not same") {
    val udtType = new UserDefinedType[Any] {
      override def sqlType: DataType = StructType(Seq(
        StructField("col1", FloatType, nullable = false),
        StructField("col2", FloatType, nullable = false)))

      override def userClass: java.lang.Class[Any] = null

      override def serialize(obj: Any): Any = null

      override def deserialize(datum: Any): Any = null
    }

    val sqlType = StructType(Seq(
      StructField("col1", FloatType, nullable = false),
      StructField("col2", IntegerType, nullable = false)))

    if (canCast(udtType.sqlType, sqlType)) {
      assertAllowed(udtType, sqlType, "m",
        "Should allow udt with compatible sqlType written to struct column")
    } else {
      val errs = new mutable.ArrayBuffer[String]()
      checkError(
        exception = intercept[AnalysisException](
          DataTypeUtils.canWrite("", udtType, sqlType, true,
            analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
        ),
        condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
        parameters = Map(
          "tableName" -> "``",
          "colName" -> "`t`.`col2`",
          "srcType" -> "\"FLOAT\"",
          "targetType" -> "\"INT\""
        )
      )
    }
  }

  test("SPARK-47528: Check udt: read side underlying sql type is not same") {
    val udtType = new UserDefinedType[Any] {
      override def sqlType: DataType = StructType(Seq(
        StructField("col1", FloatType, nullable = false),
        StructField("col2", IntegerType, nullable = false)))

      override def userClass: java.lang.Class[Any] = null

      override def serialize(obj: Any): Any = null

      override def deserialize(datum: Any): Any = null
    }

    val sqlType = StructType(Seq(
      StructField("col1", FloatType, nullable = false),
      StructField("col2", FloatType, nullable = false)))

    if (canCast(sqlType, udtType.sqlType)) {
      assertAllowed(sqlType, udtType, "m",
        "Should allow udt with compatible sqlType written to struct column")
    } else {
      val errs = new mutable.ArrayBuffer[String]()
      checkError(
        exception = intercept[AnalysisException](
          DataTypeUtils.canWrite("", sqlType, udtType, true,
            analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
        ),
        condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
        parameters = Map(
          "tableName" -> "``",
          "colName" -> "`t`.`col2`",
          "srcType" -> "\"FLOAT\"",
          "targetType" -> "\"INT\""
        )
      )
    }
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
      StructField("m", MapType(StringType, BooleanType)),
      StructField("map_of_structs", MapType(StringType, missingMiddleField)),
      StructField("y", StringType)
    ))

    val errs = new mutable.ArrayBuffer[String]()
    checkError(
      exception = intercept[AnalysisException] (
        DataTypeUtils.canWrite("", writeType, readType, true,
          analysis.caseSensitiveResolution, "t", storeAssignmentPolicy, errMsg => errs += errMsg)
      ),
      condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      parameters = Map(
        "tableName" -> "``",
        "colName" -> "`t`.`a`.`element`",
        "srcType" -> "\"STRING\"",
        "targetType" -> "\"DOUBLE\""
      )
    )
  }

  // Helper functions

  def assertAllowed(
      writeType: DataType,
      readType: DataType,
      name: String,
      desc: String,
      byName: Boolean = true): Unit = {
    assert(
      DataTypeUtils.canWrite("", writeType, readType, byName, analysis.caseSensitiveResolution,
        name, storeAssignmentPolicy,
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
      numErrs: Int,
      byName: Boolean = true)
      (checkErrors: Seq[String] => Unit): Unit = {
    val errs = new mutable.ArrayBuffer[String]()
    assert(
      DataTypeUtils.canWrite("", writeType, readType, byName, analysis.caseSensitiveResolution,
        name, storeAssignmentPolicy, errMsg => errs += errMsg) === false, desc)
    assert(errs.size === numErrs, s"Should produce $numErrs error messages")
    checkErrors(errs.toSeq)
  }
}
