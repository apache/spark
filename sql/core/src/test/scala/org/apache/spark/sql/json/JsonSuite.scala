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

package org.apache.spark.sql.json

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.decimal.Decimal
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.json.JsonRDD.{enforceCorrectType, compatibleType}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._

import java.sql.{Date, Timestamp}

class JsonSuite extends QueryTest {
  import TestJsonData._
  TestJsonData

  test("Type promotion") {
    def checkTypePromotion(expected: Any, actual: Any) {
      assert(expected.getClass == actual.getClass,
        s"Failed to promote ${actual.getClass} to ${expected.getClass}.")
      assert(expected == actual,
        s"Promoted value ${actual}(${actual.getClass}) does not equal the expected value " +
          s"${expected}(${expected.getClass}).")
    }

    val intNumber: Int = 2147483647
    checkTypePromotion(intNumber, enforceCorrectType(intNumber, IntegerType))
    checkTypePromotion(intNumber.toLong, enforceCorrectType(intNumber, LongType))
    checkTypePromotion(intNumber.toDouble, enforceCorrectType(intNumber, DoubleType))
    checkTypePromotion(
      Decimal(intNumber), enforceCorrectType(intNumber, DecimalType.Unlimited))

    val longNumber: Long = 9223372036854775807L
    checkTypePromotion(longNumber, enforceCorrectType(longNumber, LongType))
    checkTypePromotion(longNumber.toDouble, enforceCorrectType(longNumber, DoubleType))
    checkTypePromotion(
      Decimal(longNumber), enforceCorrectType(longNumber, DecimalType.Unlimited))

    val doubleNumber: Double = 1.7976931348623157E308d
    checkTypePromotion(doubleNumber.toDouble, enforceCorrectType(doubleNumber, DoubleType))
    checkTypePromotion(
      Decimal(doubleNumber), enforceCorrectType(doubleNumber, DecimalType.Unlimited))

    checkTypePromotion(new Timestamp(intNumber), enforceCorrectType(intNumber, TimestampType))
    checkTypePromotion(new Timestamp(intNumber.toLong),
        enforceCorrectType(intNumber.toLong, TimestampType))
    val strTime = "2014-09-30 12:34:56"
    checkTypePromotion(Timestamp.valueOf(strTime), enforceCorrectType(strTime, TimestampType))

    val strDate = "2014-10-15"
    checkTypePromotion(Date.valueOf(strDate), enforceCorrectType(strDate, DateType))
  }

  test("Get compatible type") {
    def checkDataType(t1: DataType, t2: DataType, expected: DataType) {
      var actual = compatibleType(t1, t2)
      assert(actual == expected,
        s"Expected $expected as the most general data type for $t1 and $t2, found $actual")
      actual = compatibleType(t2, t1)
      assert(actual == expected,
        s"Expected $expected as the most general data type for $t1 and $t2, found $actual")
    }

    // NullType
    checkDataType(NullType, BooleanType, BooleanType)
    checkDataType(NullType, IntegerType, IntegerType)
    checkDataType(NullType, LongType, LongType)
    checkDataType(NullType, DoubleType, DoubleType)
    checkDataType(NullType, DecimalType.Unlimited, DecimalType.Unlimited)
    checkDataType(NullType, StringType, StringType)
    checkDataType(NullType, ArrayType(IntegerType), ArrayType(IntegerType))
    checkDataType(NullType, StructType(Nil), StructType(Nil))
    checkDataType(NullType, NullType, NullType)

    // BooleanType
    checkDataType(BooleanType, BooleanType, BooleanType)
    checkDataType(BooleanType, IntegerType, StringType)
    checkDataType(BooleanType, LongType, StringType)
    checkDataType(BooleanType, DoubleType, StringType)
    checkDataType(BooleanType, DecimalType.Unlimited, StringType)
    checkDataType(BooleanType, StringType, StringType)
    checkDataType(BooleanType, ArrayType(IntegerType), StringType)
    checkDataType(BooleanType, StructType(Nil), StringType)

    // IntegerType
    checkDataType(IntegerType, IntegerType, IntegerType)
    checkDataType(IntegerType, LongType, LongType)
    checkDataType(IntegerType, DoubleType, DoubleType)
    checkDataType(IntegerType, DecimalType.Unlimited, DecimalType.Unlimited)
    checkDataType(IntegerType, StringType, StringType)
    checkDataType(IntegerType, ArrayType(IntegerType), StringType)
    checkDataType(IntegerType, StructType(Nil), StringType)

    // LongType
    checkDataType(LongType, LongType, LongType)
    checkDataType(LongType, DoubleType, DoubleType)
    checkDataType(LongType, DecimalType.Unlimited, DecimalType.Unlimited)
    checkDataType(LongType, StringType, StringType)
    checkDataType(LongType, ArrayType(IntegerType), StringType)
    checkDataType(LongType, StructType(Nil), StringType)

    // DoubleType
    checkDataType(DoubleType, DoubleType, DoubleType)
    checkDataType(DoubleType, DecimalType.Unlimited, DecimalType.Unlimited)
    checkDataType(DoubleType, StringType, StringType)
    checkDataType(DoubleType, ArrayType(IntegerType), StringType)
    checkDataType(DoubleType, StructType(Nil), StringType)

    // DoubleType
    checkDataType(DecimalType.Unlimited, DecimalType.Unlimited, DecimalType.Unlimited)
    checkDataType(DecimalType.Unlimited, StringType, StringType)
    checkDataType(DecimalType.Unlimited, ArrayType(IntegerType), StringType)
    checkDataType(DecimalType.Unlimited, StructType(Nil), StringType)

    // StringType
    checkDataType(StringType, StringType, StringType)
    checkDataType(StringType, ArrayType(IntegerType), StringType)
    checkDataType(StringType, StructType(Nil), StringType)

    // ArrayType
    checkDataType(ArrayType(IntegerType), ArrayType(IntegerType), ArrayType(IntegerType))
    checkDataType(ArrayType(IntegerType), ArrayType(LongType), ArrayType(LongType))
    checkDataType(ArrayType(IntegerType), ArrayType(StringType), ArrayType(StringType))
    checkDataType(ArrayType(IntegerType), StructType(Nil), StringType)
    checkDataType(
      ArrayType(IntegerType, true), ArrayType(IntegerType), ArrayType(IntegerType, true))
    checkDataType(
      ArrayType(IntegerType, true), ArrayType(IntegerType, false), ArrayType(IntegerType, true))
    checkDataType(
      ArrayType(IntegerType, true), ArrayType(IntegerType, true), ArrayType(IntegerType, true))
    checkDataType(
      ArrayType(IntegerType, false), ArrayType(IntegerType), ArrayType(IntegerType, true))
    checkDataType(
      ArrayType(IntegerType, false), ArrayType(IntegerType, false), ArrayType(IntegerType, false))
    checkDataType(
      ArrayType(IntegerType, false), ArrayType(IntegerType, true), ArrayType(IntegerType, true))

    // StructType
    checkDataType(StructType(Nil), StructType(Nil), StructType(Nil))
    checkDataType(
      StructType(StructField("f1", IntegerType, true) :: Nil),
      StructType(StructField("f1", IntegerType, true) :: Nil),
      StructType(StructField("f1", IntegerType, true) :: Nil))
    checkDataType(
      StructType(StructField("f1", IntegerType, true) :: Nil),
      StructType(Nil),
      StructType(StructField("f1", IntegerType, true) :: Nil))
    checkDataType(
      StructType(
        StructField("f1", IntegerType, true) ::
        StructField("f2", IntegerType, true) :: Nil),
      StructType(StructField("f1", LongType, true) :: Nil) ,
      StructType(
        StructField("f1", LongType, true) ::
        StructField("f2", IntegerType, true) :: Nil))
    checkDataType(
      StructType(
        StructField("f1", IntegerType, true) :: Nil),
      StructType(
        StructField("f2", IntegerType, true) :: Nil),
      StructType(
        StructField("f1", IntegerType, true) ::
        StructField("f2", IntegerType, true) :: Nil))
    checkDataType(
      StructType(
        StructField("f1", IntegerType, true) :: Nil),
      DecimalType.Unlimited,
      StringType)
  }

  test("Primitive field and type inferring") {
    val jsonSchemaRDD = jsonRDD(primitiveFieldAndType)

    val expectedSchema = StructType(
      StructField("bigInteger", DecimalType.Unlimited, true) ::
      StructField("boolean", BooleanType, true) ::
      StructField("double", DoubleType, true) ::
      StructField("integer", IntegerType, true) ::
      StructField("long", LongType, true) ::
      StructField("null", StringType, true) ::
      StructField("string", StringType, true) :: Nil)

    assert(expectedSchema === jsonSchemaRDD.schema)

    jsonSchemaRDD.registerTempTable("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      (BigDecimal("92233720368547758070"),
      true,
      1.7976931348623157E308,
      10,
      21474836470L,
      null,
      "this is a simple string.") :: Nil
    )
  }

  test("Complex field and type inferring") {
    val jsonSchemaRDD = jsonRDD(complexFieldAndType1)

    val expectedSchema = StructType(
      StructField("arrayOfArray1", ArrayType(ArrayType(StringType, false), false), true) ::
      StructField("arrayOfArray2", ArrayType(ArrayType(DoubleType, false), false), true) ::
      StructField("arrayOfBigInteger", ArrayType(DecimalType.Unlimited, false), true) ::
      StructField("arrayOfBoolean", ArrayType(BooleanType, false), true) ::
      StructField("arrayOfDouble", ArrayType(DoubleType, false), true) ::
      StructField("arrayOfInteger", ArrayType(IntegerType, false), true) ::
      StructField("arrayOfLong", ArrayType(LongType, false), true) ::
      StructField("arrayOfNull", ArrayType(StringType, true), true) ::
      StructField("arrayOfString", ArrayType(StringType, false), true) ::
      StructField("arrayOfStruct", ArrayType(
        StructType(
          StructField("field1", BooleanType, true) ::
          StructField("field2", StringType, true) ::
          StructField("field3", StringType, true) :: Nil), false), true) ::
      StructField("struct", StructType(
      StructField("field1", BooleanType, true) ::
      StructField("field2", DecimalType.Unlimited, true) :: Nil), true) ::
      StructField("structWithArrayFields", StructType(
        StructField("field1", ArrayType(IntegerType, false), true) ::
        StructField("field2", ArrayType(StringType, false), true) :: Nil), true) :: Nil)

    assert(expectedSchema === jsonSchemaRDD.schema)

    jsonSchemaRDD.registerTempTable("jsonTable")

    // Access elements of a primitive array.
    checkAnswer(
      sql("select arrayOfString[0], arrayOfString[1], arrayOfString[2] from jsonTable"),
      ("str1", "str2", null) :: Nil
    )

    // Access an array of null values.
    checkAnswer(
      sql("select arrayOfNull from jsonTable"),
      Seq(Seq(null, null, null, null)) :: Nil
    )

    // Access elements of a BigInteger array (we use DecimalType internally).
    checkAnswer(
      sql("select arrayOfBigInteger[0], arrayOfBigInteger[1], arrayOfBigInteger[2] from jsonTable"),
      (BigDecimal("922337203685477580700"), BigDecimal("-922337203685477580800"), null) :: Nil
    )

    // Access elements of an array of arrays.
    checkAnswer(
      sql("select arrayOfArray1[0], arrayOfArray1[1] from jsonTable"),
      (Seq("1", "2", "3"), Seq("str1", "str2")) :: Nil
    )

    // Access elements of an array of arrays.
    checkAnswer(
      sql("select arrayOfArray2[0], arrayOfArray2[1] from jsonTable"),
      (Seq(1.0, 2.0, 3.0), Seq(1.1, 2.1, 3.1)) :: Nil
    )

    // Access elements of an array inside a filed with the type of ArrayType(ArrayType).
    checkAnswer(
      sql("select arrayOfArray1[1][1], arrayOfArray2[1][1] from jsonTable"),
      ("str2", 2.1) :: Nil
    )

    // Access elements of an array of structs.
    checkAnswer(
      sql("select arrayOfStruct[0], arrayOfStruct[1], arrayOfStruct[2], arrayOfStruct[3] " +
        "from jsonTable"),
      (true :: "str1" :: null :: Nil,
      false :: null :: null :: Nil,
      null :: null :: null :: Nil,
      null) :: Nil
    )

    // Access a struct and fields inside of it.
    checkAnswer(
      sql("select struct, struct.field1, struct.field2 from jsonTable"),
      (
        Seq(true, BigDecimal("92233720368547758070")),
        true,
        BigDecimal("92233720368547758070")) :: Nil
    )

    // Access an array field of a struct.
    checkAnswer(
      sql("select structWithArrayFields.field1, structWithArrayFields.field2 from jsonTable"),
      (Seq(4, 5, 6), Seq("str1", "str2")) :: Nil
    )

    // Access elements of an array field of a struct.
    checkAnswer(
      sql("select structWithArrayFields.field1[1], structWithArrayFields.field2[3] from jsonTable"),
      (5, null) :: Nil
    )
  }

  ignore("Complex field and type inferring (Ignored)") {
    val jsonSchemaRDD = jsonRDD(complexFieldAndType1)
    jsonSchemaRDD.registerTempTable("jsonTable")

    // Right now, "field1" and "field2" are treated as aliases. We should fix it.
    checkAnswer(
      sql("select arrayOfStruct[0].field1, arrayOfStruct[0].field2 from jsonTable"),
      (true, "str1") :: Nil
    )

    // Right now, the analyzer cannot resolve arrayOfStruct.field1 and arrayOfStruct.field2.
    // Getting all values of a specific field from an array of structs.
    checkAnswer(
      sql("select arrayOfStruct.field1, arrayOfStruct.field2 from jsonTable"),
      (Seq(true, false), Seq("str1", null)) :: Nil
    )
  }

  test("Type conflict in primitive field values") {
    val jsonSchemaRDD = jsonRDD(primitiveFieldValueTypeConflict)

    val expectedSchema = StructType(
      StructField("num_bool", StringType, true) ::
      StructField("num_num_1", LongType, true) ::
      StructField("num_num_2", DecimalType.Unlimited, true) ::
      StructField("num_num_3", DoubleType, true) ::
      StructField("num_str", StringType, true) ::
      StructField("str_bool", StringType, true) :: Nil)

    assert(expectedSchema === jsonSchemaRDD.schema)

    jsonSchemaRDD.registerTempTable("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      ("true", 11L, null, 1.1, "13.1", "str1") ::
      ("12", null, BigDecimal("21474836470.9"), null, null, "true") ::
      ("false", 21474836470L, BigDecimal("92233720368547758070"), 100, "str1", "false") ::
      (null, 21474836570L, BigDecimal(1.1), 21474836470L, "92233720368547758070", null) :: Nil
    )

    // Number and Boolean conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_bool - 10 from jsonTable where num_bool > 11"),
      2
    )

    // Widening to LongType
    checkAnswer(
      sql("select num_num_1 - 100 from jsonTable where num_num_1 > 11"),
      Seq(21474836370L) :: Seq(21474836470L) :: Nil
    )

    checkAnswer(
      sql("select num_num_1 - 100 from jsonTable where num_num_1 > 10"),
      Seq(-89) :: Seq(21474836370L) :: Seq(21474836470L) :: Nil
    )

    // Widening to DecimalType
    checkAnswer(
      sql("select num_num_2 + 1.2 from jsonTable where num_num_2 > 1.1"),
      Seq(BigDecimal("21474836472.1")) :: Seq(BigDecimal("92233720368547758071.2")) :: Nil
    )

    // Widening to DoubleType
    checkAnswer(
      sql("select num_num_3 + 1.2 from jsonTable where num_num_3 > 1.1"),
      Seq(101.2) :: Seq(21474836471.2) :: Nil
    )

    // Number and String conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_str + 1.2 from jsonTable where num_str > 14"),
      92233720368547758071.2
    )

    // Number and String conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_str + 1.2 from jsonTable where num_str > 92233720368547758060"),
      BigDecimal("92233720368547758061.2").toDouble
    )

    // String and Boolean conflict: resolve the type as string.
    checkAnswer(
      sql("select * from jsonTable where str_bool = 'str1'"),
      ("true", 11L, null, 1.1, "13.1", "str1") :: Nil
    )
  }

  ignore("Type conflict in primitive field values (Ignored)") {
    val jsonSchemaRDD = jsonRDD(primitiveFieldValueTypeConflict)
    jsonSchemaRDD.registerTempTable("jsonTable")

    // Right now, the analyzer does not promote strings in a boolean expreesion.
    // Number and Boolean conflict: resolve the type as boolean in this query.
    checkAnswer(
      sql("select num_bool from jsonTable where NOT num_bool"),
      false
    )

    checkAnswer(
      sql("select str_bool from jsonTable where NOT str_bool"),
      false
    )

    // Right now, the analyzer does not know that num_bool should be treated as a boolean.
    // Number and Boolean conflict: resolve the type as boolean in this query.
    checkAnswer(
      sql("select num_bool from jsonTable where num_bool"),
      true
    )

    checkAnswer(
      sql("select str_bool from jsonTable where str_bool"),
      false
    )

    // The plan of the following DSL is
    // Project [(CAST(num_str#65:4, DoubleType) + 1.2) AS num#78]
    //  Filter (CAST(CAST(num_str#65:4, DoubleType), DecimalType) > 92233720368547758060)
    //    ExistingRdd [num_bool#61,num_num_1#62L,num_num_2#63,num_num_3#64,num_str#65,str_bool#66]
    // We should directly cast num_str to DecimalType and also need to do the right type promotion
    // in the Project.
    checkAnswer(
      jsonSchemaRDD.
        where('num_str > BigDecimal("92233720368547758060")).
        select('num_str + 1.2 as Symbol("num")),
      BigDecimal("92233720368547758061.2")
    )

    // The following test will fail. The type of num_str is StringType.
    // So, to evaluate num_str + 1.2, we first need to use Cast to convert the type.
    // In our test data, one value of num_str is 13.1.
    // The result of (CAST(num_str#65:4, DoubleType) + 1.2) for this value is 14.299999999999999,
    // which is not 14.3.
    // Number and String conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_str + 1.2 from jsonTable where num_str > 13"),
      Seq(14.3) :: Seq(92233720368547758071.2) :: Nil
    )
  }

  test("Type conflict in complex field values") {
    val jsonSchemaRDD = jsonRDD(complexFieldValueTypeConflict)

    val expectedSchema = StructType(
      StructField("array", ArrayType(IntegerType, false), true) ::
      StructField("num_struct", StringType, true) ::
      StructField("str_array", StringType, true) ::
      StructField("struct", StructType(
        StructField("field", StringType, true) :: Nil), true) ::
      StructField("struct_array", StringType, true) :: Nil)

    assert(expectedSchema === jsonSchemaRDD.schema)

    jsonSchemaRDD.registerTempTable("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      (Seq(), "11", "[1,2,3]", Seq(null), "[]") ::
      (null, """{"field":false}""", null, null, "{}") ::
      (Seq(4, 5, 6), null, "str", Seq(null), "[7,8,9]") ::
      (Seq(7), "{}","[str1,str2,33]", Seq("str"), """{"field":true}""") :: Nil
    )
  }

  test("Type conflict in array elements") {
    val jsonSchemaRDD = jsonRDD(arrayElementTypeConflict)

    val expectedSchema = StructType(
      StructField("array1", ArrayType(StringType, true), true) ::
      StructField("array2", ArrayType(StructType(
        StructField("field", LongType, true) :: Nil), false), true) :: Nil)

    assert(expectedSchema === jsonSchemaRDD.schema)

    jsonSchemaRDD.registerTempTable("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Seq(Seq("1", "1.1", "true", null, "[]", "{}", "[2,3,4]",
        """{"field":str}"""), Seq(Seq(214748364700L), Seq(1))) :: Nil
    )

    // Treat an element as a number.
    checkAnswer(
      sql("select array1[0] + 1 from jsonTable"),
      2
    )
  }

  test("Handling missing fields") {
    val jsonSchemaRDD = jsonRDD(missingFields)

    val expectedSchema = StructType(
      StructField("a", BooleanType, true) ::
      StructField("b", LongType, true) ::
      StructField("c", ArrayType(IntegerType, false), true) ::
      StructField("d", StructType(
        StructField("field", BooleanType, true) :: Nil), true) ::
      StructField("e", StringType, true) :: Nil)

    assert(expectedSchema === jsonSchemaRDD.schema)

    jsonSchemaRDD.registerTempTable("jsonTable")
  }

  test("Loading a JSON dataset from a text file") {
    val file = getTempFilePath("json")
    val path = file.toString
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)
    val jsonSchemaRDD = jsonFile(path)

    val expectedSchema = StructType(
      StructField("bigInteger", DecimalType.Unlimited, true) ::
      StructField("boolean", BooleanType, true) ::
      StructField("double", DoubleType, true) ::
      StructField("integer", IntegerType, true) ::
      StructField("long", LongType, true) ::
      StructField("null", StringType, true) ::
      StructField("string", StringType, true) :: Nil)

    assert(expectedSchema === jsonSchemaRDD.schema)

    jsonSchemaRDD.registerTempTable("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      (BigDecimal("92233720368547758070"),
      true,
      1.7976931348623157E308,
      10,
      21474836470L,
      null,
      "this is a simple string.") :: Nil
    )
  }

  test("Loading a JSON dataset from a text file with SQL") {
    val file = getTempFilePath("json")
    val path = file.toString
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)

    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTableSQL
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '$path'
        |)
      """.stripMargin)

    checkAnswer(
      sql("select * from jsonTableSQL"),
      (BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string.") :: Nil
    )
  }

  test("Applying schemas") {
    val file = getTempFilePath("json")
    val path = file.toString
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)

    val schema = StructType(
      StructField("bigInteger", DecimalType.Unlimited, true) ::
      StructField("boolean", BooleanType, true) ::
      StructField("double", DoubleType, true) ::
      StructField("integer", IntegerType, true) ::
      StructField("long", LongType, true) ::
      StructField("null", StringType, true) ::
      StructField("string", StringType, true) :: Nil)

    val jsonSchemaRDD1 = jsonFile(path, schema)

    assert(schema === jsonSchemaRDD1.schema)

    jsonSchemaRDD1.registerTempTable("jsonTable1")

    checkAnswer(
      sql("select * from jsonTable1"),
      (BigDecimal("92233720368547758070"),
      true,
      1.7976931348623157E308,
      10,
      21474836470L,
      null,
      "this is a simple string.") :: Nil
    )

    val jsonSchemaRDD2 = jsonRDD(primitiveFieldAndType, schema)

    assert(schema === jsonSchemaRDD2.schema)

    jsonSchemaRDD2.registerTempTable("jsonTable2")

    checkAnswer(
      sql("select * from jsonTable2"),
      (BigDecimal("92233720368547758070"),
      true,
      1.7976931348623157E308,
      10,
      21474836470L,
      null,
      "this is a simple string.") :: Nil
    )
  }

  test("SPARK-2096 Correctly parse dot notations") {
    val jsonSchemaRDD = jsonRDD(complexFieldAndType2)
    jsonSchemaRDD.registerTempTable("jsonTable")

    checkAnswer(
      sql("select arrayOfStruct[0].field1, arrayOfStruct[0].field2 from jsonTable"),
      (true, "str1") :: Nil
    )
    checkAnswer(
      sql(
        """
          |select complexArrayOfStruct[0].field1[1].inner2[0], complexArrayOfStruct[1].field2[0][1]
          |from jsonTable
        """.stripMargin),
      ("str2", 6) :: Nil
    )
  }

  test("SPARK-3390 Complex arrays") {
    val jsonSchemaRDD = jsonRDD(complexFieldAndType2)
    jsonSchemaRDD.registerTempTable("jsonTable")

    checkAnswer(
      sql(
        """
          |select arrayOfArray1[0][0][0], arrayOfArray1[1][0][1], arrayOfArray1[1][1][0]
          |from jsonTable
        """.stripMargin),
      (5, 7, 8) :: Nil
    )
    checkAnswer(
      sql(
        """
          |select arrayOfArray2[0][0][0].inner1, arrayOfArray2[1][0],
          |arrayOfArray2[1][1][1].inner2[0], arrayOfArray2[2][0][0].inner3[0][0].inner4
          |from jsonTable
        """.stripMargin),
      ("str1", Nil, "str4", 2) :: Nil
    )
  }

  test("SPARK-3308 Read top level JSON arrays") {
    val jsonSchemaRDD = jsonRDD(jsonArray)
    jsonSchemaRDD.registerTempTable("jsonTable")

    checkAnswer(
      sql(
        """
          |select a, b, c
          |from jsonTable
        """.stripMargin),
      ("str_a_1", null, null) ::
      ("str_a_2", null, null) ::
      (null, "str_b_3", null) ::
      ("str_a_4", "str_b_4", "str_c_4") :: Nil
    )
  }

  test("Corrupt records") {
    // Test if we can query corrupt records.
    val oldColumnNameOfCorruptRecord = TestSQLContext.columnNameOfCorruptRecord
    TestSQLContext.setConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD, "_unparsed")

    val jsonSchemaRDD = jsonRDD(corruptRecords)
    jsonSchemaRDD.registerTempTable("jsonTable")

    val schema = StructType(
      StructField("_unparsed", StringType, true) ::
      StructField("a", StringType, true) ::
      StructField("b", StringType, true) ::
      StructField("c", StringType, true) :: Nil)

    assert(schema === jsonSchemaRDD.schema)

    // In HiveContext, backticks should be used to access columns starting with a underscore.
    checkAnswer(
      sql(
        """
          |SELECT a, b, c, _unparsed
          |FROM jsonTable
        """.stripMargin),
      (null, null, null, "{") ::
      (null, null, null, "") ::
      (null, null, null, """{"a":1, b:2}""") ::
      (null, null, null, """{"a":{, b:3}""") ::
      ("str_a_4", "str_b_4", "str_c_4", null) ::
      (null, null, null, "]") :: Nil
    )

    checkAnswer(
      sql(
        """
          |SELECT a, b, c
          |FROM jsonTable
          |WHERE _unparsed IS NULL
        """.stripMargin),
      ("str_a_4", "str_b_4", "str_c_4") :: Nil
    )

    checkAnswer(
      sql(
        """
          |SELECT _unparsed
          |FROM jsonTable
          |WHERE _unparsed IS NOT NULL
        """.stripMargin),
      Seq("{") ::
      Seq("") ::
      Seq("""{"a":1, b:2}""") ::
      Seq("""{"a":{, b:3}""") ::
      Seq("]") :: Nil
    )

    TestSQLContext.setConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD, oldColumnNameOfCorruptRecord)
  }

  test("SPARK-4068: nulls in arrays") {
    val jsonSchemaRDD = jsonRDD(nullsInArrays)
    jsonSchemaRDD.registerTempTable("jsonTable")

    val schema = StructType(
      StructField("field1",
        ArrayType(ArrayType(ArrayType(ArrayType(StringType, false), false), true), false), true) ::
      StructField("field2",
        ArrayType(ArrayType(
          StructType(StructField("Test", IntegerType, true) :: Nil), false), true), true) ::
      StructField("field3",
        ArrayType(ArrayType(
          StructType(StructField("Test", StringType, true) :: Nil), true), false), true) ::
      StructField("field4",
        ArrayType(ArrayType(ArrayType(IntegerType, false), true), false), true) :: Nil)

    assert(schema === jsonSchemaRDD.schema)

    checkAnswer(
      sql(
        """
          |SELECT field1, field2, field3, field4
          |FROM jsonTable
        """.stripMargin),
      Seq(Seq(Seq(null), Seq(Seq(Seq("Test")))), null, null, null) ::
      Seq(null, Seq(null, Seq(Seq(1))), null, null) ::
      Seq(null, null, Seq(Seq(null), Seq(Seq("2"))), null) ::
      Seq(null, null, null, Seq(Seq(null, Seq(1, 2, 3)))) :: Nil
    )
  }
}
