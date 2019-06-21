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

package org.apache.spark.sql.execution.datasources.json

import java.io._
import java.nio.charset.{Charset, StandardCharsets, UnsupportedCharsetException}
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.util.Locale

import com.fasterxml.jackson.core.JsonFactory
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.{SparkException, TestUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{functions => F, _}
import org.apache.spark.sql.catalyst.json._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.ExternalRDD
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType.fromDDL
import org.apache.spark.util.Utils

class TestFileFilter extends PathFilter {
  override def accept(path: Path): Boolean = path.getParent.getName != "p=2"
}

class JsonSuite extends QueryTest with SharedSQLContext with TestJsonData {
  import testImplicits._

  test("Type promotion") {
    def checkTypePromotion(expected: Any, actual: Any) {
      assert(expected.getClass == actual.getClass,
        s"Failed to promote ${actual.getClass} to ${expected.getClass}.")
      assert(expected == actual,
        s"Promoted value ${actual}(${actual.getClass}) does not equal the expected value " +
          s"${expected}(${expected.getClass}).")
    }

    val factory = new JsonFactory()
    def enforceCorrectType(
        value: Any,
        dataType: DataType,
        options: Map[String, String] = Map.empty): Any = {
      val writer = new StringWriter()
      Utils.tryWithResource(factory.createGenerator(writer)) { generator =>
        generator.writeObject(value)
        generator.flush()
      }

      val dummyOption = new JSONOptions(options, SQLConf.get.sessionLocalTimeZone)
      val dummySchema = StructType(Seq.empty)
      val parser = new JacksonParser(dummySchema, dummyOption, allowArrayAsStructs = true)

      Utils.tryWithResource(factory.createParser(writer.toString)) { jsonParser =>
        jsonParser.nextToken()
        val converter = parser.makeConverter(dataType)
        converter.apply(jsonParser)
      }
    }

    val intNumber: Int = 2147483647
    checkTypePromotion(intNumber, enforceCorrectType(intNumber, IntegerType))
    checkTypePromotion(intNumber.toLong, enforceCorrectType(intNumber, LongType))
    checkTypePromotion(intNumber.toDouble, enforceCorrectType(intNumber, DoubleType))
    checkTypePromotion(
      Decimal(intNumber), enforceCorrectType(intNumber, DecimalType.SYSTEM_DEFAULT))

    val longNumber: Long = 9223372036854775807L
    checkTypePromotion(longNumber, enforceCorrectType(longNumber, LongType))
    checkTypePromotion(longNumber.toDouble, enforceCorrectType(longNumber, DoubleType))
    checkTypePromotion(
      Decimal(longNumber), enforceCorrectType(longNumber, DecimalType.SYSTEM_DEFAULT))

    val doubleNumber: Double = 1.7976931348623157E308d
    checkTypePromotion(doubleNumber.toDouble, enforceCorrectType(doubleNumber, DoubleType))

    checkTypePromotion(DateTimeUtils.fromJavaTimestamp(new Timestamp(intNumber * 1000L)),
        enforceCorrectType(intNumber, TimestampType))
    checkTypePromotion(DateTimeUtils.fromJavaTimestamp(new Timestamp(intNumber.toLong * 1000L)),
        enforceCorrectType(intNumber.toLong, TimestampType))
    val strTime = "2014-09-30 12:34:56"
    checkTypePromotion(
      expected = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(strTime)),
      enforceCorrectType(strTime, TimestampType,
        Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss")))

    val strDate = "2014-10-15"
    checkTypePromotion(
      DateTimeUtils.fromJavaDate(Date.valueOf(strDate)), enforceCorrectType(strDate, DateType))

    val ISO8601Time1 = "1970-01-01T01:00:01.0Z"
    checkTypePromotion(DateTimeUtils.fromJavaTimestamp(new Timestamp(3601000)),
        enforceCorrectType(
          ISO8601Time1,
          TimestampType,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SX")))
    val ISO8601Time2 = "1970-01-01T02:00:01-01:00"
    checkTypePromotion(DateTimeUtils.fromJavaTimestamp(new Timestamp(10801000)),
        enforceCorrectType(
          ISO8601Time2,
          TimestampType,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ssXXX")))

    val ISO8601Date = "1970-01-01"
    checkTypePromotion(DateTimeUtils.millisToDays(32400000),
      enforceCorrectType(ISO8601Date, DateType))
  }

  test("Get compatible type") {
    def checkDataType(t1: DataType, t2: DataType, expected: DataType) {
      var actual = JsonInferSchema.compatibleType(t1, t2)
      assert(actual == expected,
        s"Expected $expected as the most general data type for $t1 and $t2, found $actual")
      actual = JsonInferSchema.compatibleType(t2, t1)
      assert(actual == expected,
        s"Expected $expected as the most general data type for $t1 and $t2, found $actual")
    }

    // NullType
    checkDataType(NullType, BooleanType, BooleanType)
    checkDataType(NullType, IntegerType, IntegerType)
    checkDataType(NullType, LongType, LongType)
    checkDataType(NullType, DoubleType, DoubleType)
    checkDataType(NullType, DecimalType.SYSTEM_DEFAULT, DecimalType.SYSTEM_DEFAULT)
    checkDataType(NullType, StringType, StringType)
    checkDataType(NullType, ArrayType(IntegerType), ArrayType(IntegerType))
    checkDataType(NullType, StructType(Nil), StructType(Nil))
    checkDataType(NullType, NullType, NullType)

    // BooleanType
    checkDataType(BooleanType, BooleanType, BooleanType)
    checkDataType(BooleanType, IntegerType, StringType)
    checkDataType(BooleanType, LongType, StringType)
    checkDataType(BooleanType, DoubleType, StringType)
    checkDataType(BooleanType, DecimalType.SYSTEM_DEFAULT, StringType)
    checkDataType(BooleanType, StringType, StringType)
    checkDataType(BooleanType, ArrayType(IntegerType), StringType)
    checkDataType(BooleanType, StructType(Nil), StringType)

    // IntegerType
    checkDataType(IntegerType, IntegerType, IntegerType)
    checkDataType(IntegerType, LongType, LongType)
    checkDataType(IntegerType, DoubleType, DoubleType)
    checkDataType(IntegerType, DecimalType.SYSTEM_DEFAULT, DecimalType.SYSTEM_DEFAULT)
    checkDataType(IntegerType, StringType, StringType)
    checkDataType(IntegerType, ArrayType(IntegerType), StringType)
    checkDataType(IntegerType, StructType(Nil), StringType)

    // LongType
    checkDataType(LongType, LongType, LongType)
    checkDataType(LongType, DoubleType, DoubleType)
    checkDataType(LongType, DecimalType.SYSTEM_DEFAULT, DecimalType.SYSTEM_DEFAULT)
    checkDataType(LongType, StringType, StringType)
    checkDataType(LongType, ArrayType(IntegerType), StringType)
    checkDataType(LongType, StructType(Nil), StringType)

    // DoubleType
    checkDataType(DoubleType, DoubleType, DoubleType)
    checkDataType(DoubleType, DecimalType.SYSTEM_DEFAULT, DoubleType)
    checkDataType(DoubleType, StringType, StringType)
    checkDataType(DoubleType, ArrayType(IntegerType), StringType)
    checkDataType(DoubleType, StructType(Nil), StringType)

    // DecimalType
    checkDataType(DecimalType.SYSTEM_DEFAULT, DecimalType.SYSTEM_DEFAULT,
      DecimalType.SYSTEM_DEFAULT)
    checkDataType(DecimalType.SYSTEM_DEFAULT, StringType, StringType)
    checkDataType(DecimalType.SYSTEM_DEFAULT, ArrayType(IntegerType), StringType)
    checkDataType(DecimalType.SYSTEM_DEFAULT, StructType(Nil), StringType)

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
      StructType(StructField("f1", LongType, true) :: Nil),
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
      DecimalType.SYSTEM_DEFAULT,
      StringType)
  }

  test("Complex field and type inferring with null in sampling") {
    val jsonDF = spark.read.json(jsonNullStruct)
    val expectedSchema = StructType(
      StructField("headers", StructType(
        StructField("Charset", StringType, true) ::
          StructField("Host", StringType, true) :: Nil)
        , true) ::
        StructField("ip", StringType, true) ::
        StructField("nullstr", StringType, true):: Nil)

    assert(expectedSchema === jsonDF.schema)
    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select nullstr, headers.Host from jsonTable"),
      Seq(Row("", "1.abc.com"), Row("", null), Row("", null), Row(null, null))
    )
  }

  test("Primitive field and type inferring") {
    val jsonDF = spark.read.json(primitiveFieldAndType)

    val expectedSchema = StructType(
      StructField("bigInteger", DecimalType(20, 0), true) ::
      StructField("boolean", BooleanType, true) ::
      StructField("double", DoubleType, true) ::
      StructField("integer", LongType, true) ::
      StructField("long", LongType, true) ::
      StructField("null", StringType, true) ::
      StructField("string", StringType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Row(new java.math.BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string.")
    )
  }

  test("Complex field and type inferring") {
    val jsonDF = spark.read.json(complexFieldAndType1)

    val expectedSchema = StructType(
      StructField("arrayOfArray1", ArrayType(ArrayType(StringType, true), true), true) ::
      StructField("arrayOfArray2", ArrayType(ArrayType(DoubleType, true), true), true) ::
      StructField("arrayOfBigInteger", ArrayType(DecimalType(21, 0), true), true) ::
      StructField("arrayOfBoolean", ArrayType(BooleanType, true), true) ::
      StructField("arrayOfDouble", ArrayType(DoubleType, true), true) ::
      StructField("arrayOfInteger", ArrayType(LongType, true), true) ::
      StructField("arrayOfLong", ArrayType(LongType, true), true) ::
      StructField("arrayOfNull", ArrayType(StringType, true), true) ::
      StructField("arrayOfString", ArrayType(StringType, true), true) ::
      StructField("arrayOfStruct", ArrayType(
        StructType(
          StructField("field1", BooleanType, true) ::
          StructField("field2", StringType, true) ::
          StructField("field3", StringType, true) :: Nil), true), true) ::
      StructField("struct", StructType(
        StructField("field1", BooleanType, true) ::
        StructField("field2", DecimalType(20, 0), true) :: Nil), true) ::
      StructField("structWithArrayFields", StructType(
        StructField("field1", ArrayType(LongType, true), true) ::
        StructField("field2", ArrayType(StringType, true), true) :: Nil), true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    // Access elements of a primitive array.
    checkAnswer(
      sql("select arrayOfString[0], arrayOfString[1], arrayOfString[2] from jsonTable"),
      Row("str1", "str2", null)
    )

    // Access an array of null values.
    checkAnswer(
      sql("select arrayOfNull from jsonTable"),
      Row(Seq(null, null, null, null))
    )

    // Access elements of a BigInteger array (we use DecimalType internally).
    checkAnswer(
      sql("select arrayOfBigInteger[0], arrayOfBigInteger[1], arrayOfBigInteger[2] from jsonTable"),
      Row(new java.math.BigDecimal("922337203685477580700"),
        new java.math.BigDecimal("-922337203685477580800"), null)
    )

    // Access elements of an array of arrays.
    checkAnswer(
      sql("select arrayOfArray1[0], arrayOfArray1[1] from jsonTable"),
      Row(Seq("1", "2", "3"), Seq("str1", "str2"))
    )

    // Access elements of an array of arrays.
    checkAnswer(
      sql("select arrayOfArray2[0], arrayOfArray2[1] from jsonTable"),
      Row(Seq(1.0, 2.0, 3.0), Seq(1.1, 2.1, 3.1))
    )

    // Access elements of an array inside a filed with the type of ArrayType(ArrayType).
    checkAnswer(
      sql("select arrayOfArray1[1][1], arrayOfArray2[1][1] from jsonTable"),
      Row("str2", 2.1)
    )

    // Access elements of an array of structs.
    checkAnswer(
      sql("select arrayOfStruct[0], arrayOfStruct[1], arrayOfStruct[2], arrayOfStruct[3] " +
        "from jsonTable"),
      Row(
        Row(true, "str1", null),
        Row(false, null, null),
        Row(null, null, null),
        null)
    )

    // Access a struct and fields inside of it.
    checkAnswer(
      sql("select struct, struct.field1, struct.field2 from jsonTable"),
      Row(
        Row(true, new java.math.BigDecimal("92233720368547758070")),
        true,
        new java.math.BigDecimal("92233720368547758070")) :: Nil
    )

    // Access an array field of a struct.
    checkAnswer(
      sql("select structWithArrayFields.field1, structWithArrayFields.field2 from jsonTable"),
      Row(Seq(4, 5, 6), Seq("str1", "str2"))
    )

    // Access elements of an array field of a struct.
    checkAnswer(
      sql("select structWithArrayFields.field1[1], structWithArrayFields.field2[3] from jsonTable"),
      Row(5, null)
    )
  }

  test("GetField operation on complex data type") {
    val jsonDF = spark.read.json(complexFieldAndType1)
    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select arrayOfStruct[0].field1, arrayOfStruct[0].field2 from jsonTable"),
      Row(true, "str1")
    )

    // Getting all values of a specific field from an array of structs.
    checkAnswer(
      sql("select arrayOfStruct.field1, arrayOfStruct.field2 from jsonTable"),
      Row(Seq(true, false, null), Seq("str1", null, null))
    )
  }

  test("Type conflict in primitive field values") {
    val jsonDF = spark.read.json(primitiveFieldValueTypeConflict)

    val expectedSchema = StructType(
      StructField("num_bool", StringType, true) ::
      StructField("num_num_1", LongType, true) ::
      StructField("num_num_2", DoubleType, true) ::
      StructField("num_num_3", DoubleType, true) ::
      StructField("num_str", StringType, true) ::
      StructField("str_bool", StringType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Row("true", 11L, null, 1.1, "13.1", "str1") ::
        Row("12", null, 21474836470.9, null, null, "true") ::
        Row("false", 21474836470L, 92233720368547758070d, 100, "str1", "false") ::
        Row(null, 21474836570L, 1.1, 21474836470L, "92233720368547758070", null) :: Nil
    )

    // Number and Boolean conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_bool - 10 from jsonTable where num_bool > 11"),
      Row(2)
    )

    // Widening to LongType
    checkAnswer(
      sql("select num_num_1 - 100 from jsonTable where num_num_1 > 11"),
      Row(21474836370L) :: Row(21474836470L) :: Nil
    )

    checkAnswer(
      sql("select num_num_1 - 100 from jsonTable where num_num_1 > 10"),
      Row(-89) :: Row(21474836370L) :: Row(21474836470L) :: Nil
    )

    // Widening to DecimalType
    checkAnswer(
      sql("select num_num_2 + 1.3 from jsonTable where num_num_2 > 1.1"),
      Row(21474836472.2) ::
        Row(92233720368547758071.3) :: Nil
    )

    // Widening to Double
    checkAnswer(
      sql("select num_num_3 + 1.2 from jsonTable where num_num_3 > 1.1"),
      Row(101.2) :: Row(21474836471.2) :: Nil
    )

    // Number and String conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_str + 1.2 from jsonTable where num_str > 14d"),
      Row(92233720368547758071.2)
    )

    // Number and String conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_str + 1.2 from jsonTable where num_str >= 92233720368547758060"),
      Row(new java.math.BigDecimal("92233720368547758071.2").doubleValue)
    )

    // String and Boolean conflict: resolve the type as string.
    checkAnswer(
      sql("select * from jsonTable where str_bool = 'str1'"),
      Row("true", 11L, null, 1.1, "13.1", "str1")
    )
  }

  ignore("Type conflict in primitive field values (Ignored)") {
    val jsonDF = spark.read.json(primitiveFieldValueTypeConflict)
    jsonDF.createOrReplaceTempView("jsonTable")

    // Right now, the analyzer does not promote strings in a boolean expression.
    // Number and Boolean conflict: resolve the type as boolean in this query.
    checkAnswer(
      sql("select num_bool from jsonTable where NOT num_bool"),
      Row(false)
    )

    checkAnswer(
      sql("select str_bool from jsonTable where NOT str_bool"),
      Row(false)
    )

    // Right now, the analyzer does not know that num_bool should be treated as a boolean.
    // Number and Boolean conflict: resolve the type as boolean in this query.
    checkAnswer(
      sql("select num_bool from jsonTable where num_bool"),
      Row(true)
    )

    checkAnswer(
      sql("select str_bool from jsonTable where str_bool"),
      Row(false)
    )

    // The plan of the following DSL is
    // Project [(CAST(num_str#65:4, DoubleType) + 1.2) AS num#78]
    //  Filter (CAST(CAST(num_str#65:4, DoubleType), DecimalType) > 92233720368547758060)
    //    ExistingRdd [num_bool#61,num_num_1#62L,num_num_2#63,num_num_3#64,num_str#65,str_bool#66]
    // We should directly cast num_str to DecimalType and also need to do the right type promotion
    // in the Project.
    checkAnswer(
      jsonDF.
        where('num_str >= BigDecimal("92233720368547758060")).
        select(('num_str + 1.2).as("num")),
      Row(new java.math.BigDecimal("92233720368547758071.2").doubleValue())
    )

    // The following test will fail. The type of num_str is StringType.
    // So, to evaluate num_str + 1.2, we first need to use Cast to convert the type.
    // In our test data, one value of num_str is 13.1.
    // The result of (CAST(num_str#65:4, DoubleType) + 1.2) for this value is 14.299999999999999,
    // which is not 14.3.
    // Number and String conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_str + 1.2 from jsonTable where num_str > 13"),
      Row(BigDecimal("14.3")) :: Row(BigDecimal("92233720368547758071.2")) :: Nil
    )
  }

  test("Type conflict in complex field values") {
    val jsonDF = spark.read.json(complexFieldValueTypeConflict)

    val expectedSchema = StructType(
      StructField("array", ArrayType(LongType, true), true) ::
      StructField("num_struct", StringType, true) ::
      StructField("str_array", StringType, true) ::
      StructField("struct", StructType(
        StructField("field", StringType, true) :: Nil), true) ::
      StructField("struct_array", StringType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Row(Seq(), "11", "[1,2,3]", Row(null), "[]") ::
        Row(null, """{"field":false}""", null, null, "{}") ::
        Row(Seq(4, 5, 6), null, "str", Row(null), "[7,8,9]") ::
        Row(Seq(7), "{}", """["str1","str2",33]""", Row("str"), """{"field":true}""") :: Nil
    )
  }

  test("Type conflict in array elements") {
    val jsonDF = spark.read.json(arrayElementTypeConflict)

    val expectedSchema = StructType(
      StructField("array1", ArrayType(StringType, true), true) ::
      StructField("array2", ArrayType(StructType(
        StructField("field", LongType, true) :: Nil), true), true) ::
      StructField("array3", ArrayType(StringType, true), true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Row(Seq("1", "1.1", "true", null, "[]", "{}", "[2,3,4]",
        """{"field":"str"}"""), Seq(Row(214748364700L), Row(1)), null) ::
      Row(null, null, Seq("""{"field":"str"}""", """{"field":1}""")) ::
      Row(null, null, Seq("1", "2", "3")) :: Nil
    )

    // Treat an element as a number.
    checkAnswer(
      sql("select array1[0] + 1 from jsonTable where array1 is not null"),
      Row(2)
    )
  }

  test("Handling missing fields") {
    val jsonDF = spark.read.json(missingFields)

    val expectedSchema = StructType(
      StructField("a", BooleanType, true) ::
      StructField("b", LongType, true) ::
      StructField("c", ArrayType(LongType, true), true) ::
      StructField("d", StructType(
        StructField("field", BooleanType, true) :: Nil), true) ::
      StructField("e", StringType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")
  }

  test("Loading a JSON dataset from a text file") {
    val dir = Utils.createTempDir()
    dir.delete()
    val path = dir.getCanonicalPath
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).write.text(path)
    val jsonDF = spark.read.json(path)

    val expectedSchema = StructType(
      StructField("bigInteger", DecimalType(20, 0), true) ::
      StructField("boolean", BooleanType, true) ::
      StructField("double", DoubleType, true) ::
      StructField("integer", LongType, true) ::
      StructField("long", LongType, true) ::
      StructField("null", StringType, true) ::
      StructField("string", StringType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Row(new java.math.BigDecimal("92233720368547758070"),
      true,
      1.7976931348623157E308,
      10,
      21474836470L,
      null,
      "this is a simple string.")
    )
  }

  test("Loading a JSON dataset primitivesAsString returns schema with primitive types as strings") {
    val dir = Utils.createTempDir()
    dir.delete()
    val path = dir.getCanonicalPath
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).write.text(path)
    val jsonDF = spark.read.option("primitivesAsString", "true").json(path)

    val expectedSchema = StructType(
      StructField("bigInteger", StringType, true) ::
      StructField("boolean", StringType, true) ::
      StructField("double", StringType, true) ::
      StructField("integer", StringType, true) ::
      StructField("long", StringType, true) ::
      StructField("null", StringType, true) ::
      StructField("string", StringType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Row("92233720368547758070",
      "true",
      "1.7976931348623157E308",
      "10",
      "21474836470",
      null,
      "this is a simple string.")
    )
  }

  test("Loading a JSON dataset primitivesAsString returns complex fields as strings") {
    val jsonDF = spark.read.option("primitivesAsString", "true").json(complexFieldAndType1)

    val expectedSchema = StructType(
      StructField("arrayOfArray1", ArrayType(ArrayType(StringType, true), true), true) ::
      StructField("arrayOfArray2", ArrayType(ArrayType(StringType, true), true), true) ::
      StructField("arrayOfBigInteger", ArrayType(StringType, true), true) ::
      StructField("arrayOfBoolean", ArrayType(StringType, true), true) ::
      StructField("arrayOfDouble", ArrayType(StringType, true), true) ::
      StructField("arrayOfInteger", ArrayType(StringType, true), true) ::
      StructField("arrayOfLong", ArrayType(StringType, true), true) ::
      StructField("arrayOfNull", ArrayType(StringType, true), true) ::
      StructField("arrayOfString", ArrayType(StringType, true), true) ::
      StructField("arrayOfStruct", ArrayType(
        StructType(
          StructField("field1", StringType, true) ::
          StructField("field2", StringType, true) ::
          StructField("field3", StringType, true) :: Nil), true), true) ::
      StructField("struct", StructType(
        StructField("field1", StringType, true) ::
        StructField("field2", StringType, true) :: Nil), true) ::
      StructField("structWithArrayFields", StructType(
        StructField("field1", ArrayType(StringType, true), true) ::
        StructField("field2", ArrayType(StringType, true), true) :: Nil), true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    // Access elements of a primitive array.
    checkAnswer(
      sql("select arrayOfString[0], arrayOfString[1], arrayOfString[2] from jsonTable"),
      Row("str1", "str2", null)
    )

    // Access an array of null values.
    checkAnswer(
      sql("select arrayOfNull from jsonTable"),
      Row(Seq(null, null, null, null))
    )

    // Access elements of a BigInteger array (we use DecimalType internally).
    checkAnswer(
      sql("select arrayOfBigInteger[0], arrayOfBigInteger[1], arrayOfBigInteger[2] from jsonTable"),
      Row("922337203685477580700", "-922337203685477580800", null)
    )

    // Access elements of an array of arrays.
    checkAnswer(
      sql("select arrayOfArray1[0], arrayOfArray1[1] from jsonTable"),
      Row(Seq("1", "2", "3"), Seq("str1", "str2"))
    )

    // Access elements of an array of arrays.
    checkAnswer(
      sql("select arrayOfArray2[0], arrayOfArray2[1] from jsonTable"),
      Row(Seq("1", "2", "3"), Seq("1.1", "2.1", "3.1"))
    )

    // Access elements of an array inside a filed with the type of ArrayType(ArrayType).
    checkAnswer(
      sql("select arrayOfArray1[1][1], arrayOfArray2[1][1] from jsonTable"),
      Row("str2", "2.1")
    )

    // Access elements of an array of structs.
    checkAnswer(
      sql("select arrayOfStruct[0], arrayOfStruct[1], arrayOfStruct[2], arrayOfStruct[3] " +
        "from jsonTable"),
      Row(
        Row("true", "str1", null),
        Row("false", null, null),
        Row(null, null, null),
        null)
    )

    // Access a struct and fields inside of it.
    checkAnswer(
      sql("select struct, struct.field1, struct.field2 from jsonTable"),
      Row(
        Row("true", "92233720368547758070"),
        "true",
        "92233720368547758070") :: Nil
    )

    // Access an array field of a struct.
    checkAnswer(
      sql("select structWithArrayFields.field1, structWithArrayFields.field2 from jsonTable"),
      Row(Seq("4", "5", "6"), Seq("str1", "str2"))
    )

    // Access elements of an array field of a struct.
    checkAnswer(
      sql("select structWithArrayFields.field1[1], structWithArrayFields.field2[3] from jsonTable"),
      Row("5", null)
    )
  }

  test("Loading a JSON dataset prefersDecimal returns schema with float types as BigDecimal") {
    val jsonDF = spark.read.option("prefersDecimal", "true").json(primitiveFieldAndType)

    val expectedSchema = StructType(
      StructField("bigInteger", DecimalType(20, 0), true) ::
        StructField("boolean", BooleanType, true) ::
        StructField("double", DecimalType(17, -292), true) ::
        StructField("integer", LongType, true) ::
        StructField("long", LongType, true) ::
        StructField("null", StringType, true) ::
        StructField("string", StringType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Row(BigDecimal("92233720368547758070"),
        true,
        BigDecimal("1.7976931348623157E308"),
        10,
        21474836470L,
        null,
        "this is a simple string.")
    )
  }

  test("Find compatible types even if inferred DecimalType is not capable of other IntegralType") {
    val mixedIntegerAndDoubleRecords = Seq(
      """{"a": 3, "b": 1.1}""",
      s"""{"a": 3.1, "b": 0.${"0" * 38}1}""").toDS()
    val jsonDF = spark.read
      .option("prefersDecimal", "true")
      .json(mixedIntegerAndDoubleRecords)

    // The values in `a` field will be decimals as they fit in decimal. For `b` field,
    // they will be doubles as `1.0E-39D` does not fit.
    val expectedSchema = StructType(
      StructField("a", DecimalType(21, 1), true) ::
      StructField("b", DoubleType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)
    checkAnswer(
      jsonDF,
      Row(BigDecimal("3"), 1.1D) ::
      Row(BigDecimal("3.1"), 1.0E-39D) :: Nil
    )
  }

  test("Infer big integers correctly even when it does not fit in decimal") {
    val jsonDF = spark.read
      .json(bigIntegerRecords)

    // The value in `a` field will be a double as it does not fit in decimal. For `b` field,
    // it will be a decimal as `92233720368547758070`.
    val expectedSchema = StructType(
      StructField("a", DoubleType, true) ::
      StructField("b", DecimalType(20, 0), true) :: Nil)

    assert(expectedSchema === jsonDF.schema)
    checkAnswer(jsonDF, Row(1.0E38D, BigDecimal("92233720368547758070")))
  }

  test("Infer floating-point values correctly even when it does not fit in decimal") {
    val jsonDF = spark.read
      .option("prefersDecimal", "true")
      .json(floatingValueRecords)

    // The value in `a` field will be a double as it does not fit in decimal. For `b` field,
    // it will be a decimal as `0.01` by having a precision equal to the scale.
    val expectedSchema = StructType(
      StructField("a", DoubleType, true) ::
      StructField("b", DecimalType(2, 2), true):: Nil)

    assert(expectedSchema === jsonDF.schema)
    checkAnswer(jsonDF, Row(1.0E-39D, BigDecimal("0.01")))

    val mergedJsonDF = spark.read
      .option("prefersDecimal", "true")
      .json(floatingValueRecords.union(bigIntegerRecords))

    val expectedMergedSchema = StructType(
      StructField("a", DoubleType, true) ::
      StructField("b", DecimalType(22, 2), true):: Nil)

    assert(expectedMergedSchema === mergedJsonDF.schema)
    checkAnswer(
      mergedJsonDF,
      Row(1.0E-39D, BigDecimal("0.01")) ::
      Row(1.0E38D, BigDecimal("92233720368547758070")) :: Nil
    )
  }

  test("Loading a JSON dataset from a text file with SQL") {
    val dir = Utils.createTempDir()
    dir.delete()
    val path = dir.toURI.toString
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).write.text(path)

    sql(
      s"""
        |CREATE TEMPORARY VIEW jsonTableSQL
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '$path'
        |)
      """.stripMargin)

    checkAnswer(
      sql("select * from jsonTableSQL"),
      Row(new java.math.BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string.")
    )
  }

  test("Applying schemas") {
    val dir = Utils.createTempDir()
    dir.delete()
    val path = dir.getCanonicalPath
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).write.text(path)

    val schema = StructType(
      StructField("bigInteger", DecimalType.SYSTEM_DEFAULT, true) ::
      StructField("boolean", BooleanType, true) ::
      StructField("double", DoubleType, true) ::
      StructField("integer", IntegerType, true) ::
      StructField("long", LongType, true) ::
      StructField("null", StringType, true) ::
      StructField("string", StringType, true) :: Nil)

    val jsonDF1 = spark.read.schema(schema).json(path)

    assert(schema === jsonDF1.schema)

    jsonDF1.createOrReplaceTempView("jsonTable1")

    checkAnswer(
      sql("select * from jsonTable1"),
      Row(new java.math.BigDecimal("92233720368547758070"),
      true,
      1.7976931348623157E308,
      10,
      21474836470L,
      null,
      "this is a simple string.")
    )

    val jsonDF2 = spark.read.schema(schema).json(primitiveFieldAndType)

    assert(schema === jsonDF2.schema)

    jsonDF2.createOrReplaceTempView("jsonTable2")

    checkAnswer(
      sql("select * from jsonTable2"),
      Row(new java.math.BigDecimal("92233720368547758070"),
      true,
      1.7976931348623157E308,
      10,
      21474836470L,
      null,
      "this is a simple string.")
    )
  }

  test("Applying schemas with MapType") {
    val schemaWithSimpleMap = StructType(
      StructField("map", MapType(StringType, IntegerType, true), false) :: Nil)
    val jsonWithSimpleMap = spark.read.schema(schemaWithSimpleMap).json(mapType1)

    jsonWithSimpleMap.createOrReplaceTempView("jsonWithSimpleMap")

    checkAnswer(
      sql("select `map` from jsonWithSimpleMap"),
      Row(Map("a" -> 1)) ::
      Row(Map("b" -> 2)) ::
      Row(Map("c" -> 3)) ::
      Row(Map("c" -> 1, "d" -> 4)) ::
      Row(Map("e" -> null)) :: Nil
    )

    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
      checkAnswer(
        sql("select `map`['c'] from jsonWithSimpleMap"),
        Row(null) ::
        Row(null) ::
        Row(3) ::
        Row(1) ::
        Row(null) :: Nil
      )
    }

    val innerStruct = StructType(
      StructField("field1", ArrayType(IntegerType, true), true) ::
      StructField("field2", IntegerType, true) :: Nil)
    val schemaWithComplexMap = StructType(
      StructField("map", MapType(StringType, innerStruct, true), false) :: Nil)

    val jsonWithComplexMap = spark.read.schema(schemaWithComplexMap).json(mapType2)

    jsonWithComplexMap.createOrReplaceTempView("jsonWithComplexMap")

    checkAnswer(
      sql("select `map` from jsonWithComplexMap"),
      Row(Map("a" -> Row(Seq(1, 2, 3, null), null))) ::
      Row(Map("b" -> Row(null, 2))) ::
      Row(Map("c" -> Row(Seq(), 4))) ::
      Row(Map("c" -> Row(null, 3), "d" -> Row(Seq(null), null))) ::
      Row(Map("e" -> null)) ::
      Row(Map("f" -> Row(null, null))) :: Nil
    )

    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
      checkAnswer(
        sql("select `map`['a'].field1, `map`['c'].field2 from jsonWithComplexMap"),
        Row(Seq(1, 2, 3, null), null) ::
        Row(null, null) ::
        Row(null, 4) ::
        Row(null, 3) ::
        Row(null, null) ::
        Row(null, null) :: Nil
      )
    }
  }

  test("SPARK-2096 Correctly parse dot notations") {
    val jsonDF = spark.read.json(complexFieldAndType2)
    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql("select arrayOfStruct[0].field1, arrayOfStruct[0].field2 from jsonTable"),
      Row(true, "str1")
    )
    checkAnswer(
      sql(
        """
          |select complexArrayOfStruct[0].field1[1].inner2[0], complexArrayOfStruct[1].field2[0][1]
          |from jsonTable
        """.stripMargin),
      Row("str2", 6)
    )
  }

  test("SPARK-3390 Complex arrays") {
    val jsonDF = spark.read.json(complexFieldAndType2)
    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql(
        """
          |select arrayOfArray1[0][0][0], arrayOfArray1[1][0][1], arrayOfArray1[1][1][0]
          |from jsonTable
        """.stripMargin),
      Row(5, 7, 8)
    )
    checkAnswer(
      sql(
        """
          |select arrayOfArray2[0][0][0].inner1, arrayOfArray2[1][0],
          |arrayOfArray2[1][1][1].inner2[0], arrayOfArray2[2][0][0].inner3[0][0].inner4
          |from jsonTable
        """.stripMargin),
      Row("str1", Nil, "str4", 2)
    )
  }

  test("SPARK-3308 Read top level JSON arrays") {
    val jsonDF = spark.read.json(jsonArray)
    jsonDF.createOrReplaceTempView("jsonTable")

    checkAnswer(
      sql(
        """
          |select a, b, c
          |from jsonTable
        """.stripMargin),
      Row("str_a_1", null, null) ::
        Row("str_a_2", null, null) ::
        Row(null, "str_b_3", null) ::
        Row("str_a_4", "str_b_4", "str_c_4") :: Nil
    )
  }

  test("Corrupt records: FAILFAST mode") {
    // `FAILFAST` mode should throw an exception for corrupt records.
    val exceptionOne = intercept[SparkException] {
      spark.read
        .option("mode", "FAILFAST")
        .json(corruptRecords)
    }.getMessage
    assert(exceptionOne.contains(
      "Malformed records are detected in schema inference. Parse Mode: FAILFAST."))

    val exceptionTwo = intercept[SparkException] {
      spark.read
        .option("mode", "FAILFAST")
        .schema("a string")
        .json(corruptRecords)
        .collect()
    }.getMessage
    assert(exceptionTwo.contains(
      "Malformed records are detected in record parsing. Parse Mode: FAILFAST."))
  }

  test("Corrupt records: DROPMALFORMED mode") {
    val schemaOne = StructType(
      StructField("a", StringType, true) ::
        StructField("b", StringType, true) ::
        StructField("c", StringType, true) :: Nil)
    val schemaTwo = StructType(
      StructField("a", StringType, true) :: Nil)
    // `DROPMALFORMED` mode should skip corrupt records
    val jsonDFOne = spark.read
      .option("mode", "DROPMALFORMED")
      .json(corruptRecords)
    checkAnswer(
      jsonDFOne,
      Row("str_a_4", "str_b_4", "str_c_4") :: Nil
    )
    assert(jsonDFOne.schema === schemaOne)

    val jsonDFTwo = spark.read
      .option("mode", "DROPMALFORMED")
      .schema(schemaTwo)
      .json(corruptRecords)
    checkAnswer(
      jsonDFTwo,
      Row("str_a_4") :: Nil)
    assert(jsonDFTwo.schema === schemaTwo)
  }

  test("SPARK-19641: Additional corrupt records: DROPMALFORMED mode") {
    val schema = new StructType().add("dummy", StringType)
    // `DROPMALFORMED` mode should skip corrupt records
    val jsonDF = spark.read
      .option("mode", "DROPMALFORMED")
      .json(additionalCorruptRecords)
    checkAnswer(
      jsonDF,
      Row("test"))
    assert(jsonDF.schema === schema)
  }

  test("Corrupt records: PERMISSIVE mode, without designated column for malformed records") {
    val schema = StructType(
      StructField("a", StringType, true) ::
        StructField("b", StringType, true) ::
        StructField("c", StringType, true) :: Nil)

    val jsonDF = spark.read.schema(schema).json(corruptRecords)

    checkAnswer(
      jsonDF.select($"a", $"b", $"c"),
      Seq(
        // Corrupted records are replaced with null
        Row(null, null, null),
        Row(null, null, null),
        Row(null, null, null),
        Row("str_a_4", "str_b_4", "str_c_4"),
        Row(null, null, null))
    )
  }

  test("Corrupt records: PERMISSIVE mode, with designated column for malformed records") {
    // Test if we can query corrupt records.
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      val jsonDF = spark.read.json(corruptRecords)
      val schema = StructType(
        StructField("_unparsed", StringType, true) ::
          StructField("a", StringType, true) ::
          StructField("b", StringType, true) ::
          StructField("c", StringType, true) :: Nil)

      assert(schema === jsonDF.schema)

      // In HiveContext, backticks should be used to access columns starting with a underscore.
      checkAnswer(
        jsonDF.select($"a", $"b", $"c", $"_unparsed"),
        Row(null, null, null, "{") ::
          Row(null, null, null, """{"a":1, b:2}""") ::
          Row(null, null, null, """{"a":{, b:3}""") ::
          Row("str_a_4", "str_b_4", "str_c_4", null) ::
          Row(null, null, null, "]") :: Nil
      )

      checkAnswer(
        jsonDF.filter($"_unparsed".isNull).select($"a", $"b", $"c"),
        Row("str_a_4", "str_b_4", "str_c_4")
      )

      checkAnswer(
        jsonDF.filter($"_unparsed".isNotNull).select($"_unparsed"),
        Row("{") ::
          Row("""{"a":1, b:2}""") ::
          Row("""{"a":{, b:3}""") ::
          Row("]") :: Nil
      )
    }
  }

  test("SPARK-13953 Rename the corrupt record field via option") {
    val jsonDF = spark.read
      .option("columnNameOfCorruptRecord", "_malformed")
      .json(corruptRecords)
    val schema = StructType(
      StructField("_malformed", StringType, true) ::
        StructField("a", StringType, true) ::
        StructField("b", StringType, true) ::
        StructField("c", StringType, true) :: Nil)

    assert(schema === jsonDF.schema)
    checkAnswer(
      jsonDF.selectExpr("a", "b", "c", "_malformed"),
      Row(null, null, null, "{") ::
        Row(null, null, null, """{"a":1, b:2}""") ::
        Row(null, null, null, """{"a":{, b:3}""") ::
        Row("str_a_4", "str_b_4", "str_c_4", null) ::
        Row(null, null, null, "]") :: Nil
    )
  }

  test("SPARK-4068: nulls in arrays") {
    val jsonDF = spark.read.json(nullsInArrays)
    jsonDF.createOrReplaceTempView("jsonTable")

    val schema = StructType(
      StructField("field1",
        ArrayType(ArrayType(ArrayType(ArrayType(StringType, true), true), true), true), true) ::
      StructField("field2",
        ArrayType(ArrayType(
          StructType(StructField("Test", LongType, true) :: Nil), true), true), true) ::
      StructField("field3",
        ArrayType(ArrayType(
          StructType(StructField("Test", StringType, true) :: Nil), true), true), true) ::
      StructField("field4",
        ArrayType(ArrayType(ArrayType(LongType, true), true), true), true) :: Nil)

    assert(schema === jsonDF.schema)

    checkAnswer(
      sql(
        """
          |SELECT field1, field2, field3, field4
          |FROM jsonTable
        """.stripMargin),
      Row(Seq(Seq(null), Seq(Seq(Seq("Test")))), null, null, null) ::
        Row(null, Seq(null, Seq(Row(1))), null, null) ::
        Row(null, null, Seq(Seq(null), Seq(Row("2"))), null) ::
        Row(null, null, null, Seq(Seq(null, Seq(1, 2, 3)))) :: Nil
    )
  }

  test("SPARK-4228 DataFrame to JSON") {
    val schema1 = StructType(
      StructField("f1", IntegerType, false) ::
      StructField("f2", StringType, false) ::
      StructField("f3", BooleanType, false) ::
      StructField("f4", ArrayType(StringType), nullable = true) ::
      StructField("f5", IntegerType, true) :: Nil)

    val rowRDD1 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v5 = try values(3).toInt catch {
        case _: NumberFormatException => null
      }
      Row(values(0).toInt, values(1), values(2).toBoolean, r.split(",").toList, v5)
    }

    val df1 = spark.createDataFrame(rowRDD1, schema1)
    df1.createOrReplaceTempView("applySchema1")
    val df2 = df1.toDF
    val result = df2.toJSON.collect()
    // scalastyle:off
    assert(result(0) === "{\"f1\":1,\"f2\":\"A1\",\"f3\":true,\"f4\":[\"1\",\" A1\",\" true\",\" null\"]}")
    assert(result(3) === "{\"f1\":4,\"f2\":\"D4\",\"f3\":true,\"f4\":[\"4\",\" D4\",\" true\",\" 2147483644\"],\"f5\":2147483644}")
    // scalastyle:on

    val schema2 = StructType(
      StructField("f1", StructType(
        StructField("f11", IntegerType, false) ::
        StructField("f12", BooleanType, false) :: Nil), false) ::
      StructField("f2", MapType(StringType, IntegerType, true), false) :: Nil)

    val rowRDD2 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v4 = try values(3).toInt catch {
        case _: NumberFormatException => null
      }
      Row(Row(values(0).toInt, values(2).toBoolean), Map(values(1) -> v4))
    }

    val df3 = spark.createDataFrame(rowRDD2, schema2)
    df3.createOrReplaceTempView("applySchema2")
    val df4 = df3.toDF
    val result2 = df4.toJSON.collect()

    assert(result2(1) === "{\"f1\":{\"f11\":2,\"f12\":false},\"f2\":{\"B2\":null}}")
    assert(result2(3) === "{\"f1\":{\"f11\":4,\"f12\":true},\"f2\":{\"D4\":2147483644}}")

    val jsonDF = spark.read.json(primitiveFieldAndType)
    val primTable = spark.read.json(jsonDF.toJSON)
    primTable.createOrReplaceTempView("primitiveTable")
    checkAnswer(
        sql("select * from primitiveTable"),
      Row(new java.math.BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,
        "this is a simple string.")
      )

    val complexJsonDF = spark.read.json(complexFieldAndType1)
    val compTable = spark.read.json(complexJsonDF.toJSON)
    compTable.createOrReplaceTempView("complexTable")
    // Access elements of a primitive array.
    checkAnswer(
      sql("select arrayOfString[0], arrayOfString[1], arrayOfString[2] from complexTable"),
      Row("str1", "str2", null)
    )

    // Access an array of null values.
    checkAnswer(
      sql("select arrayOfNull from complexTable"),
      Row(Seq(null, null, null, null))
    )

    // Access elements of a BigInteger array (we use DecimalType internally).
    checkAnswer(
      sql("select arrayOfBigInteger[0], arrayOfBigInteger[1], arrayOfBigInteger[2] " +
        " from complexTable"),
      Row(new java.math.BigDecimal("922337203685477580700"),
        new java.math.BigDecimal("-922337203685477580800"), null)
    )

    // Access elements of an array of arrays.
    checkAnswer(
      sql("select arrayOfArray1[0], arrayOfArray1[1] from complexTable"),
      Row(Seq("1", "2", "3"), Seq("str1", "str2"))
    )

    // Access elements of an array of arrays.
    checkAnswer(
      sql("select arrayOfArray2[0], arrayOfArray2[1] from complexTable"),
      Row(Seq(1.0, 2.0, 3.0), Seq(1.1, 2.1, 3.1))
    )

    // Access elements of an array inside a filed with the type of ArrayType(ArrayType).
    checkAnswer(
      sql("select arrayOfArray1[1][1], arrayOfArray2[1][1] from complexTable"),
      Row("str2", 2.1)
    )

    // Access a struct and fields inside of it.
    checkAnswer(
      sql("select struct, struct.field1, struct.field2 from complexTable"),
      Row(
        Row(true, new java.math.BigDecimal("92233720368547758070")),
        true,
        new java.math.BigDecimal("92233720368547758070")) :: Nil
    )

    // Access an array field of a struct.
    checkAnswer(
      sql("select structWithArrayFields.field1, structWithArrayFields.field2 from complexTable"),
      Row(Seq(4, 5, 6), Seq("str1", "str2"))
    )

    // Access elements of an array field of a struct.
    checkAnswer(
      sql("select structWithArrayFields.field1[1], structWithArrayFields.field2[3] " +
        "from complexTable"),
      Row(5, null)
    )
  }

  test("Dataset toJSON doesn't construct rdd") {
    val containsRDD = spark.emptyDataFrame.toJSON.queryExecution.logical.find {
      case ExternalRDD(_, _) => true
      case _ => false
    }

    assert(containsRDD.isEmpty, "Expected logical plan of toJSON to not contain an RDD")
  }

  test("JSONRelation equality test") {
    withTempPath(dir => {
      val path = dir.getCanonicalFile.toURI.toString
      sparkContext.parallelize(1 to 100)
        .map(i => s"""{"a": 1, "b": "str$i"}""").saveAsTextFile(path)

      val d1 = DataSource(
        spark,
        userSpecifiedSchema = None,
        partitionColumns = Array.empty[String],
        bucketSpec = None,
        className = classOf[JsonFileFormat].getCanonicalName,
        options = Map("path" -> path)).resolveRelation()

      val d2 = DataSource(
        spark,
        userSpecifiedSchema = None,
        partitionColumns = Array.empty[String],
        bucketSpec = None,
        className = classOf[JsonFileFormat].getCanonicalName,
        options = Map("path" -> path)).resolveRelation()
      assert(d1 === d2)
    })
  }

  test("SPARK-6245 JsonInferSchema.infer on empty RDD") {
    // This is really a test that it doesn't throw an exception
    val options = new JSONOptions(Map.empty[String, String], "GMT")
    val emptySchema = new JsonInferSchema(options).infer(
      empty.rdd,
      CreateJacksonParser.string)
    assert(StructType(Seq()) === emptySchema)
  }

  test("SPARK-7565 MapType in JsonRDD") {
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      withTempDir { dir =>
        val schemaWithSimpleMap = StructType(
          StructField("map", MapType(StringType, IntegerType, true), false) :: Nil)
        val df = spark.read.schema(schemaWithSimpleMap).json(mapType1)

        val path = dir.getAbsolutePath
        df.write.mode("overwrite").parquet(path)
        // order of MapType is not defined
        assert(spark.read.parquet(path).count() == 5)

        val df2 = spark.read.json(corruptRecords)
        df2.write.mode("overwrite").parquet(path)
        checkAnswer(spark.read.parquet(path), df2.collect())
      }
    }
  }

  test("SPARK-8093 Erase empty structs") {
    val options = new JSONOptions(Map.empty[String, String], "GMT")
    val emptySchema = new JsonInferSchema(options).infer(
      emptyRecords.rdd,
      CreateJacksonParser.string)
    assert(StructType(Seq()) === emptySchema)
  }

  test("JSON with Partition") {
    def makePartition(rdd: RDD[String], parent: File, partName: String, partValue: Any): File = {
      val p = new File(parent, s"$partName=${partValue.toString}")
      rdd.saveAsTextFile(p.getCanonicalPath)
      p
    }

    withTempPath(root => {
      val d1 = new File(root, "d1=1")
      // root/dt=1/col1=abc
      val p1_col1 = makePartition(
        sparkContext.parallelize(2 to 5).map(i => s"""{"a": 1, "b": "str$i"}"""),
        d1,
        "col1",
        "abc")

      // root/dt=1/col1=abd
      val p2 = makePartition(
        sparkContext.parallelize(6 to 10).map(i => s"""{"a": 1, "b": "str$i"}"""),
        d1,
        "col1",
        "abd")

        spark.read.json(root.getAbsolutePath).createOrReplaceTempView("test_myjson_with_part")
        checkAnswer(sql(
          "SELECT count(a) FROM test_myjson_with_part where d1 = 1 and col1='abc'"), Row(4))
        checkAnswer(sql(
          "SELECT count(a) FROM test_myjson_with_part where d1 = 1 and col1='abd'"), Row(5))
        checkAnswer(sql(
          "SELECT count(a) FROM test_myjson_with_part where d1 = 1"), Row(9))
    })
  }

  test("SPARK-11544 test pathfilter") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = spark.range(2)
      df.write.json(path + "/p=1")
      df.write.json(path + "/p=2")
      assert(spark.read.json(path).count() === 4)

      val extraOptions = Map(
        "mapred.input.pathFilter.class" -> classOf[TestFileFilter].getName,
        "mapreduce.input.pathFilter.class" -> classOf[TestFileFilter].getName
      )
      assert(spark.read.options(extraOptions).json(path).count() === 2)
    }
  }

  test("SPARK-12057 additional corrupt records do not throw exceptions") {
    // Test if we can query corrupt records.
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      withTempView("jsonTable") {
        val schema = StructType(
          StructField("_unparsed", StringType, true) ::
            StructField("dummy", StringType, true) :: Nil)

        {
          // We need to make sure we can infer the schema.
          val jsonDF = spark.read.json(additionalCorruptRecords)
          assert(jsonDF.schema === schema)
        }

        {
          val jsonDF = spark.read.schema(schema).json(additionalCorruptRecords)
          jsonDF.createOrReplaceTempView("jsonTable")

          // In HiveContext, backticks should be used to access columns starting with a underscore.
          checkAnswer(
            sql(
              """
                |SELECT dummy, _unparsed
                |FROM jsonTable
              """.stripMargin),
            Row("test", null) ::
              Row(null, """[1,2,3]""") ::
              Row(null, """":"test", "a":1}""") ::
              Row(null, """42""") ::
              Row(null, """     ","ian":"test"}""") :: Nil
          )
        }
      }
    }
  }

  test("Parse JSON rows having an array type and a struct type in the same field.") {
    withTempDir { dir =>
      val dir = Utils.createTempDir()
      dir.delete()
      val path = dir.getCanonicalPath
      arrayAndStructRecords.map(record => record.replaceAll("\n", " ")).write.text(path)

      val schema =
        StructType(
          StructField("a", StructType(
            StructField("b", StringType) :: Nil
          )) :: Nil)
      val jsonDF = spark.read.schema(schema).json(path)
      assert(jsonDF.count() == 2)
    }
  }

  test("SPARK-12872 Support to specify the option for compression codec") {
    withTempDir { dir =>
      val dir = Utils.createTempDir()
      dir.delete()
      val path = dir.getCanonicalPath
      primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).write.text(path)

      val jsonDF = spark.read.json(path)
      val jsonDir = new File(dir, "json").getCanonicalPath
      jsonDF.coalesce(1).write
        .format("json")
        .option("compression", "gZiP")
        .save(jsonDir)

      val compressedFiles = new File(jsonDir).listFiles()
      assert(compressedFiles.exists(_.getName.endsWith(".json.gz")))

      val jsonCopy = spark.read
        .format("json")
        .load(jsonDir)

      assert(jsonCopy.count == jsonDF.count)
      val jsonCopySome = jsonCopy.selectExpr("string", "long", "boolean")
      val jsonDFSome = jsonDF.selectExpr("string", "long", "boolean")
      checkAnswer(jsonCopySome, jsonDFSome)
    }
  }

  test("SPARK-13543 Write the output as uncompressed via option()") {
    val extraOptions = Map[String, String](
      "mapreduce.output.fileoutputformat.compress" -> "true",
      "mapreduce.output.fileoutputformat.compress.type" -> CompressionType.BLOCK.toString,
      "mapreduce.output.fileoutputformat.compress.codec" -> classOf[GzipCodec].getName,
      "mapreduce.map.output.compress" -> "true",
      "mapreduce.map.output.compress.codec" -> classOf[GzipCodec].getName
    )
    withTempDir { dir =>
      val dir = Utils.createTempDir()
      dir.delete()

      val path = dir.getCanonicalPath
      primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).write.text(path)

      val jsonDF = spark.read.json(path)
      val jsonDir = new File(dir, "json").getCanonicalPath
      jsonDF.coalesce(1).write
        .format("json")
        .option("compression", "none")
        .options(extraOptions)
        .save(jsonDir)

      val compressedFiles = new File(jsonDir).listFiles()
      assert(compressedFiles.exists(!_.getName.endsWith(".json.gz")))

      val jsonCopy = spark.read
        .format("json")
        .options(extraOptions)
        .load(jsonDir)

      assert(jsonCopy.count == jsonDF.count)
      val jsonCopySome = jsonCopy.selectExpr("string", "long", "boolean")
      val jsonDFSome = jsonDF.selectExpr("string", "long", "boolean")
      checkAnswer(jsonCopySome, jsonDFSome)
    }
  }

  test("Casting long as timestamp") {
    withTempView("jsonTable") {
      val schema = (new StructType).add("ts", TimestampType)
      val jsonDF = spark.read.schema(schema).json(timestampAsLong)

      jsonDF.createOrReplaceTempView("jsonTable")

      checkAnswer(
        sql("select ts from jsonTable"),
        Row(java.sql.Timestamp.valueOf("2016-01-02 03:04:05"))
      )
    }
  }

  test("wide nested json table") {
    val nested = (1 to 100).map { i =>
      s"""
         |"c$i": $i
       """.stripMargin
    }.mkString(", ")
    val json = s"""
       |{"a": [{$nested}], "b": [{$nested}]}
     """.stripMargin
    val df = spark.read.json(Seq(json).toDS())
    assert(df.schema.size === 2)
    df.collect()
  }

  test("Write dates correctly with dateFormat option") {
    val customSchema = new StructType(Array(StructField("date", DateType, true)))
    withTempDir { dir =>
      // With dateFormat option.
      val datesWithFormatPath = s"${dir.getCanonicalPath}/datesWithFormat.json"
      val datesWithFormat = spark.read
        .schema(customSchema)
        .option("dateFormat", "dd/MM/yyyy HH:mm")
        .json(datesRecords)

      datesWithFormat.write
        .format("json")
        .option("dateFormat", "yyyy/MM/dd")
        .save(datesWithFormatPath)

      // This will load back the dates as string.
      val stringSchema = StructType(StructField("date", StringType, true) :: Nil)
      val stringDatesWithFormat = spark.read
        .schema(stringSchema)
        .json(datesWithFormatPath)
      val expectedStringDatesWithFormat = Seq(
        Row("2015/08/26"),
        Row("2014/10/27"),
        Row("2016/01/28"))

      checkAnswer(stringDatesWithFormat, expectedStringDatesWithFormat)
    }
  }

  test("Write timestamps correctly with timestampFormat option") {
    val customSchema = new StructType(Array(StructField("date", TimestampType, true)))
    withTempDir { dir =>
      // With dateFormat option.
      val timestampsWithFormatPath = s"${dir.getCanonicalPath}/timestampsWithFormat.json"
      val timestampsWithFormat = spark.read
        .schema(customSchema)
        .option("timestampFormat", "dd/MM/yyyy HH:mm")
        .json(datesRecords)
      timestampsWithFormat.write
        .format("json")
        .option("timestampFormat", "yyyy/MM/dd HH:mm")
        .save(timestampsWithFormatPath)

      // This will load back the timestamps as string.
      val stringSchema = StructType(StructField("date", StringType, true) :: Nil)
      val stringTimestampsWithFormat = spark.read
        .schema(stringSchema)
        .json(timestampsWithFormatPath)
      val expectedStringDatesWithFormat = Seq(
        Row("2015/08/26 18:00"),
        Row("2014/10/27 18:30"),
        Row("2016/01/28 20:00"))

      checkAnswer(stringTimestampsWithFormat, expectedStringDatesWithFormat)
    }
  }

  test("Write timestamps correctly with timestampFormat option and timeZone option") {
    val customSchema = new StructType(Array(StructField("date", TimestampType, true)))
    withTempDir { dir =>
      // With dateFormat option and timeZone option.
      val timestampsWithFormatPath = s"${dir.getCanonicalPath}/timestampsWithFormat.json"
      val timestampsWithFormat = spark.read
        .schema(customSchema)
        .option("timestampFormat", "dd/MM/yyyy HH:mm")
        .json(datesRecords)
      timestampsWithFormat.write
        .format("json")
        .option("timestampFormat", "yyyy/MM/dd HH:mm")
        .option(DateTimeUtils.TIMEZONE_OPTION, "GMT")
        .save(timestampsWithFormatPath)

      // This will load back the timestamps as string.
      val stringSchema = StructType(StructField("date", StringType, true) :: Nil)
      val stringTimestampsWithFormat = spark.read
        .schema(stringSchema)
        .json(timestampsWithFormatPath)
      val expectedStringDatesWithFormat = Seq(
        Row("2015/08/27 01:00"),
        Row("2014/10/28 01:30"),
        Row("2016/01/29 04:00"))

      checkAnswer(stringTimestampsWithFormat, expectedStringDatesWithFormat)

      val readBack = spark.read
        .schema(customSchema)
        .option("timestampFormat", "yyyy/MM/dd HH:mm")
        .option(DateTimeUtils.TIMEZONE_OPTION, "GMT")
        .json(timestampsWithFormatPath)

      checkAnswer(readBack, timestampsWithFormat)
    }
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val records = Seq("""{"a": 3, "b": 1.1}""", """{"a": 3.1, "b": 0.000001}""").toDS()

    val schema = StructType(
      StructField("a", DecimalType(21, 1), true) ::
      StructField("b", DecimalType(7, 6), true) :: Nil)

    val df1 = spark.read.option("prefersDecimal", "true").json(records)
    assert(df1.schema == schema)
    val df2 = spark.read.option("PREfersdecimaL", "true").json(records)
    assert(df2.schema == schema)
  }

  test("SPARK-18352: Parse normal multi-line JSON files (compressed)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      primitiveFieldAndType
        .toDF("value")
        .write
        .option("compression", "GzIp")
        .text(path)

      assert(new File(path).listFiles().exists(_.getName.endsWith(".gz")))

      val jsonDF = spark.read.option("multiLine", true).json(path)
      val jsonDir = new File(dir, "json").getCanonicalPath
      jsonDF.coalesce(1).write
        .option("compression", "gZiP")
        .json(jsonDir)

      assert(new File(jsonDir).listFiles().exists(_.getName.endsWith(".json.gz")))

      val originalData = spark.read.json(primitiveFieldAndType)
      checkAnswer(jsonDF, originalData)
      checkAnswer(spark.read.schema(originalData.schema).json(jsonDir), originalData)
    }
  }

  test("SPARK-18352: Parse normal multi-line JSON files (uncompressed)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      primitiveFieldAndType
        .toDF("value")
        .write
        .text(path)

      val jsonDF = spark.read.option("multiLine", true).json(path)
      val jsonDir = new File(dir, "json").getCanonicalPath
      jsonDF.coalesce(1).write.json(jsonDir)

      val compressedFiles = new File(jsonDir).listFiles()
      assert(compressedFiles.exists(_.getName.endsWith(".json")))

      val originalData = spark.read.json(primitiveFieldAndType)
      checkAnswer(jsonDF, originalData)
      checkAnswer(spark.read.schema(originalData.schema).json(jsonDir), originalData)
    }
  }

  test("SPARK-18352: Expect one JSON document per file") {
    // the json parser terminates as soon as it sees a matching END_OBJECT or END_ARRAY token.
    // this might not be the optimal behavior but this test verifies that only the first value
    // is parsed and the rest are discarded.

    // alternatively the parser could continue parsing following objects, which may further reduce
    // allocations by skipping the line reader entirely

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark
        .createDataFrame(Seq(Tuple1("{}{invalid}")))
        .coalesce(1)
        .write
        .text(path)

      val jsonDF = spark.read.option("multiLine", true).json(path)
      // no corrupt record column should be created
      assert(jsonDF.schema === StructType(Seq()))
      // only the first object should be read
      assert(jsonDF.count() === 1)
    }
  }

  test("SPARK-18352: Handle multi-line corrupt documents (PERMISSIVE)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val corruptRecordCount = additionalCorruptRecords.count().toInt
      assert(corruptRecordCount === 5)

      additionalCorruptRecords
        .toDF("value")
        // this is the minimum partition count that avoids hash collisions
        .repartition(corruptRecordCount * 4, F.hash($"value"))
        .write
        .text(path)

      val jsonDF = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json(path)
      assert(jsonDF.count() === corruptRecordCount)
      assert(jsonDF.schema === new StructType()
        .add("_corrupt_record", StringType)
        .add("dummy", StringType))
      val counts = jsonDF
        .join(
          additionalCorruptRecords.toDF("value"),
          F.regexp_replace($"_corrupt_record", "(^\\s+|\\s+$)", "") === F.trim($"value"),
          "outer")
        .agg(
          F.count($"dummy").as("valid"),
          F.count($"_corrupt_record").as("corrupt"),
          F.count("*").as("count"))
      checkAnswer(counts, Row(1, 4, 6))
    }
  }

  test("SPARK-19641: Handle multi-line corrupt documents (DROPMALFORMED)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val corruptRecordCount = additionalCorruptRecords.count().toInt
      assert(corruptRecordCount === 5)

      additionalCorruptRecords
        .toDF("value")
        // this is the minimum partition count that avoids hash collisions
        .repartition(corruptRecordCount * 4, F.hash($"value"))
        .write
        .text(path)

      val jsonDF = spark.read.option("multiLine", true).option("mode", "DROPMALFORMED").json(path)
      checkAnswer(jsonDF, Seq(Row("test")))
    }
  }

  test("SPARK-18352: Handle multi-line corrupt documents (FAILFAST)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val corruptRecordCount = additionalCorruptRecords.count().toInt
      assert(corruptRecordCount === 5)

      additionalCorruptRecords
        .toDF("value")
        // this is the minimum partition count that avoids hash collisions
        .repartition(corruptRecordCount * 4, F.hash($"value"))
        .write
        .text(path)

      val schema = new StructType().add("dummy", StringType)

      // `FAILFAST` mode should throw an exception for corrupt records.
      val exceptionOne = intercept[SparkException] {
        spark.read
          .option("multiLine", true)
          .option("mode", "FAILFAST")
          .json(path)
      }
      assert(exceptionOne.getMessage.contains("Malformed records are detected in schema " +
        "inference. Parse Mode: FAILFAST."))

      val exceptionTwo = intercept[SparkException] {
        spark.read
          .option("multiLine", true)
          .option("mode", "FAILFAST")
          .schema(schema)
          .json(path)
          .collect()
      }
      assert(exceptionTwo.getMessage.contains("Malformed records are detected in record " +
        "parsing. Parse Mode: FAILFAST."))
    }
  }

  test("Throw an exception if a `columnNameOfCorruptRecord` field violates requirements") {
    val columnNameOfCorruptRecord = "_unparsed"
    val schema = StructType(
      StructField(columnNameOfCorruptRecord, IntegerType, true) ::
        StructField("a", StringType, true) ::
        StructField("b", StringType, true) ::
        StructField("c", StringType, true) :: Nil)
    val errMsg = intercept[AnalysisException] {
      spark.read
        .option("mode", "Permissive")
        .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
        .schema(schema)
        .json(corruptRecords)
    }.getMessage
    assert(errMsg.startsWith("The field for corrupt records must be string type and nullable"))

    // We use `PERMISSIVE` mode by default if invalid string is given.
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      corruptRecords.toDF("value").write.text(path)
      val errMsg = intercept[AnalysisException] {
        spark.read
          .option("mode", "permm")
          .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
          .schema(schema)
          .json(path)
          .collect
      }.getMessage
      assert(errMsg.startsWith("The field for corrupt records must be string type and nullable"))
    }
  }

  test("SPARK-18772: Parse special floats correctly") {
    val jsons = Seq(
      """{"a": "NaN"}""",
      """{"a": "Infinity"}""",
      """{"a": "-Infinity"}""")

    // positive cases
    val checks: Seq[Double => Boolean] = Seq(
      _.isNaN,
      _.isPosInfinity,
      _.isNegInfinity)

    Seq(FloatType, DoubleType).foreach { dt =>
      jsons.zip(checks).foreach { case (json, check) =>
        val ds = spark.read
          .schema(StructType(Seq(StructField("a", dt))))
          .json(Seq(json).toDS())
          .select($"a".cast(DoubleType)).as[Double]
        assert(check(ds.first()))
      }
    }

    // negative cases
    Seq(FloatType, DoubleType).foreach { dt =>
      val lowerCasedJsons = jsons.map(_.toLowerCase(Locale.ROOT))
      // The special floats are case-sensitive so these cases below throw exceptions.
      lowerCasedJsons.foreach { lowerCasedJson =>
        val e = intercept[SparkException] {
          spark.read
            .option("mode", "FAILFAST")
            .schema(StructType(Seq(StructField("a", dt))))
            .json(Seq(lowerCasedJson).toDS())
            .collect()
        }
        assert(e.getMessage.contains("Cannot parse"))
      }
    }
  }

  test("SPARK-21610: Corrupt records are not handled properly when creating a dataframe " +
    "from a file") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val data =
        """{"field": 1}
          |{"field": 2}
          |{"field": "3"}""".stripMargin
      Seq(data).toDF().repartition(1).write.text(path)
      val schema = new StructType().add("field", ByteType).add("_corrupt_record", StringType)
      // negative cases
      val msg = intercept[AnalysisException] {
        spark.read.schema(schema).json(path).select("_corrupt_record").collect()
      }.getMessage
      assert(msg.contains("only include the internal corrupt record column"))

      // workaround
      val df = spark.read.schema(schema).json(path).cache()
      assert(df.filter($"_corrupt_record".isNotNull).count() == 1)
      assert(df.filter($"_corrupt_record".isNull).count() == 2)
      checkAnswer(
        df.select("_corrupt_record"),
        Row(null) :: Row(null) :: Row("{\"field\": \"3\"}") :: Nil
      )
    }
  }

  def testLineSeparator(lineSep: String): Unit = {
    test(s"SPARK-21289: Support line separator - lineSep: '$lineSep'") {
      // Read
      val data =
        s"""
          |  {"f":
          |"a", "f0": 1}$lineSep{"f":
          |
          |"c",  "f0": 2}$lineSep{"f": "d",  "f0": 3}
        """.stripMargin
      val dataWithTrailingLineSep = s"$data$lineSep"

      Seq(data, dataWithTrailingLineSep).foreach { lines =>
        withTempPath { path =>
          Files.write(path.toPath, lines.getBytes(StandardCharsets.UTF_8))
          val df = spark.read.option("lineSep", lineSep).json(path.getAbsolutePath)
          val expectedSchema =
            StructType(StructField("f", StringType) :: StructField("f0", LongType) :: Nil)
          checkAnswer(df, Seq(("a", 1), ("c", 2), ("d", 3)).toDF())
          assert(df.schema === expectedSchema)
        }
      }

      // Write
      withTempPath { path =>
        Seq("a", "b", "c").toDF("value").coalesce(1)
          .write.option("lineSep", lineSep).json(path.getAbsolutePath)
        val partFile = TestUtils.recursiveList(path).filter(f => f.getName.startsWith("part-")).head
        val readBack = new String(Files.readAllBytes(partFile.toPath), StandardCharsets.UTF_8)
        assert(
          readBack === s"""{"value":"a"}$lineSep{"value":"b"}$lineSep{"value":"c"}$lineSep""")
      }

      // Roundtrip
      withTempPath { path =>
        val df = Seq("a", "b", "c").toDF()
        df.write.option("lineSep", lineSep).json(path.getAbsolutePath)
        val readBack = spark.read.option("lineSep", lineSep).json(path.getAbsolutePath)
        checkAnswer(df, readBack)
      }
    }
  }

  // scalastyle:off nonascii
  Seq("|", "^", "::", "!!!@3", 0x1E.toChar.toString, "아").foreach { lineSep =>
    testLineSeparator(lineSep)
  }
  // scalastyle:on nonascii

  test("""SPARK-21289: Support line separator - default value \r, \r\n and \n""") {
    val data =
      "{\"f\": \"a\", \"f0\": 1}\r{\"f\": \"c\",  \"f0\": 2}\r\n{\"f\": \"d\",  \"f0\": 3}\n"

    withTempPath { path =>
      Files.write(path.toPath, data.getBytes(StandardCharsets.UTF_8))
      val df = spark.read.json(path.getAbsolutePath)
      val expectedSchema =
        StructType(StructField("f", StringType) :: StructField("f0", LongType) :: Nil)
      checkAnswer(df, Seq(("a", 1), ("c", 2), ("d", 3)).toDF())
      assert(df.schema === expectedSchema)
    }
  }

  test("SPARK-23849: schema inferring touches less data if samplingRatio < 1.0") {
    // Set default values for the DataSource parameters to make sure
    // that whole test file is mapped to only one partition. This will guarantee
    // reliable sampling of the input file.
    withSQLConf(
      "spark.sql.files.maxPartitionBytes" -> (128 * 1024 * 1024).toString,
      "spark.sql.files.openCostInBytes" -> (4 * 1024 * 1024).toString
    )(withTempPath { path =>
      val ds = sampledTestData.coalesce(1)
      ds.write.text(path.getAbsolutePath)
      val readback = spark.read.option("samplingRatio", 0.1).json(path.getCanonicalPath)

      assert(readback.schema == new StructType().add("f1", LongType))
    })
  }

  test("SPARK-23849: usage of samplingRatio while parsing a dataset of strings") {
    val ds = sampledTestData.coalesce(1)
    val readback = spark.read.option("samplingRatio", 0.1).json(ds)

    assert(readback.schema == new StructType().add("f1", LongType))
  }

  test("SPARK-23849: samplingRatio is out of the range (0, 1.0]") {
    val ds = spark.range(0, 100, 1, 1).map(_.toString)

    val errorMsg0 = intercept[IllegalArgumentException] {
      spark.read.option("samplingRatio", -1).json(ds)
    }.getMessage
    assert(errorMsg0.contains("samplingRatio (-1.0) should be greater than 0"))

    val errorMsg1 = intercept[IllegalArgumentException] {
      spark.read.option("samplingRatio", 0).json(ds)
    }.getMessage
    assert(errorMsg1.contains("samplingRatio (0.0) should be greater than 0"))

    val sampled = spark.read.option("samplingRatio", 1.0).json(ds)
    assert(sampled.count() == ds.count())
  }

  test("SPARK-23723: json in UTF-16 with BOM") {
    val fileName = "test-data/utf16WithBOM.json"
    val schema = new StructType().add("firstName", StringType).add("lastName", StringType)
    val jsonDF = spark.read.schema(schema)
      .option("multiline", "true")
      .option("encoding", "UTF-16")
      .json(testFile(fileName))

    checkAnswer(jsonDF, Seq(Row("Chris", "Baird"), Row("Doug", "Rood")))
  }

  test("SPARK-23723: multi-line json in UTF-32BE with BOM") {
    val fileName = "test-data/utf32BEWithBOM.json"
    val schema = new StructType().add("firstName", StringType).add("lastName", StringType)
    val jsonDF = spark.read.schema(schema)
      .option("multiline", "true")
      .json(testFile(fileName))

    checkAnswer(jsonDF, Seq(Row("Chris", "Baird")))
  }

  test("SPARK-23723: Use user's encoding in reading of multi-line json in UTF-16LE") {
    val fileName = "test-data/utf16LE.json"
    val schema = new StructType().add("firstName", StringType).add("lastName", StringType)
    val jsonDF = spark.read.schema(schema)
      .option("multiline", "true")
      .options(Map("encoding" -> "UTF-16LE"))
      .json(testFile(fileName))

    checkAnswer(jsonDF, Seq(Row("Chris", "Baird")))
  }

  test("SPARK-23723: Unsupported encoding name") {
    val invalidCharset = "UTF-128"
    val exception = intercept[UnsupportedCharsetException] {
      spark.read
        .options(Map("encoding" -> invalidCharset, "lineSep" -> "\n"))
        .json(testFile("test-data/utf16LE.json"))
        .count()
    }

    assert(exception.getMessage.contains(invalidCharset))
  }

  test("SPARK-23723: checking that the encoding option is case agnostic") {
    val fileName = "test-data/utf16LE.json"
    val schema = new StructType().add("firstName", StringType).add("lastName", StringType)
    val jsonDF = spark.read.schema(schema)
      .option("multiline", "true")
      .options(Map("encoding" -> "uTf-16lE"))
      .json(testFile(fileName))

    checkAnswer(jsonDF, Seq(Row("Chris", "Baird")))
  }

  test("SPARK-23723: specified encoding is not matched to actual encoding") {
    val fileName = "test-data/utf16LE.json"
    val schema = new StructType().add("firstName", StringType).add("lastName", StringType)
    val exception = intercept[SparkException] {
      spark.read.schema(schema)
        .option("mode", "FAILFAST")
        .option("multiline", "true")
        .options(Map("encoding" -> "UTF-16BE"))
        .json(testFile(fileName))
        .count()
    }
    val errMsg = exception.getMessage

    assert(errMsg.contains("Malformed records are detected in record parsing"))
  }

  def checkEncoding(expectedEncoding: String, pathToJsonFiles: String,
      expectedContent: String): Unit = {
    val jsonFiles = new File(pathToJsonFiles)
      .listFiles()
      .filter(_.isFile)
      .filter(_.getName.endsWith("json"))
    val actualContent = jsonFiles.map { file =>
      new String(Files.readAllBytes(file.toPath), expectedEncoding)
    }.mkString.trim

    assert(actualContent == expectedContent)
  }

  test("SPARK-23723: save json in UTF-32BE") {
    val encoding = "UTF-32BE"
    withTempPath { path =>
      val df = spark.createDataset(Seq(("Dog", 42)))
      df.write
        .options(Map("encoding" -> encoding))
        .json(path.getCanonicalPath)

      checkEncoding(
        expectedEncoding = encoding,
        pathToJsonFiles = path.getCanonicalPath,
        expectedContent = """{"_1":"Dog","_2":42}""")
    }
  }

  test("SPARK-23723: save json in default encoding - UTF-8") {
    withTempPath { path =>
      val df = spark.createDataset(Seq(("Dog", 42)))
      df.write.json(path.getCanonicalPath)

      checkEncoding(
        expectedEncoding = "UTF-8",
        pathToJsonFiles = path.getCanonicalPath,
        expectedContent = """{"_1":"Dog","_2":42}""")
    }
  }

  test("SPARK-23723: wrong output encoding") {
    val encoding = "UTF-128"
    val exception = intercept[SparkException] {
      withTempPath { path =>
        val df = spark.createDataset(Seq((0)))
        df.write
          .options(Map("encoding" -> encoding))
          .json(path.getCanonicalPath)
      }
    }

    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true, "UTF-8")
    exception.printStackTrace(ps)
    ps.flush()

    assert(baos.toString.contains(
      "java.nio.charset.UnsupportedCharsetException: UTF-128"))
  }

  test("SPARK-23723: read back json in UTF-16LE") {
    val options = Map("encoding" -> "UTF-16LE", "lineSep" -> "\n")
    withTempPath { path =>
      val ds = spark.createDataset(Seq(("a", 1), ("b", 2), ("c", 3))).repartition(2)
      ds.write.options(options).json(path.getCanonicalPath)

      val readBack = spark
        .read
        .options(options)
        .json(path.getCanonicalPath)

      checkAnswer(readBack.toDF(), ds.toDF())
    }
  }

  test("SPARK-23723: write json in UTF-16/32 with multiline off") {
    Seq("UTF-16", "UTF-32").foreach { encoding =>
      withTempPath { path =>
        val ds = spark.createDataset(Seq(("a", 1))).repartition(1)
        ds.write
          .option("encoding", encoding)
          .option("multiline", false)
          .json(path.getCanonicalPath)
        val jsonFiles = path.listFiles().filter(_.getName.endsWith("json"))
        jsonFiles.foreach { jsonFile =>
          val readback = Files.readAllBytes(jsonFile.toPath)
          val expected = ("""{"_1":"a","_2":1}""" + "\n").getBytes(Charset.forName(encoding))
          assert(readback === expected)
        }
      }
    }
  }

  def checkReadJson(lineSep: String, encoding: String, inferSchema: Boolean, id: Int): Unit = {
    test(s"SPARK-23724: checks reading json in ${encoding} #${id}") {
      val schema = new StructType().add("f1", StringType).add("f2", IntegerType)
      withTempPath { path =>
        val records = List(("a", 1), ("b", 2))
        val data = records
          .map(rec => s"""{"f1":"${rec._1}", "f2":${rec._2}}""".getBytes(encoding))
          .reduce((a1, a2) => a1 ++ lineSep.getBytes(encoding) ++ a2)
        val os = new FileOutputStream(path)
        os.write(data)
        os.close()
        val reader = if (inferSchema) {
          spark.read
        } else {
          spark.read.schema(schema)
        }
        val readBack = reader
          .option("encoding", encoding)
          .option("lineSep", lineSep)
          .json(path.getCanonicalPath)
        checkAnswer(readBack, records.map(rec => Row(rec._1, rec._2)))
      }
    }
  }

  // scalastyle:off nonascii
  List(
    (0, "|", "UTF-8", false),
    (1, "^", "UTF-16BE", true),
    (2, "::", "ISO-8859-1", true),
    (3, "!!!@3", "UTF-32LE", false),
    (4, 0x1E.toChar.toString, "UTF-8", true),
    (5, "아", "UTF-32BE", false),
    (6, "куку", "CP1251", true),
    (7, "sep", "utf-8", false),
    (8, "\r\n", "UTF-16LE", false),
    (9, "\r\n", "utf-16be", true),
    (10, "\u000d\u000a", "UTF-32BE", false),
    (11, "\u000a\u000d", "UTF-8", true),
    (12, "===", "US-ASCII", false),
    (13, "$^+", "utf-32le", true)
  ).foreach {
    case (testNum, sep, encoding, inferSchema) => checkReadJson(sep, encoding, inferSchema, testNum)
  }
  // scalastyle:on nonascii

  test("SPARK-23724: lineSep should be set if encoding if different from UTF-8") {
    val encoding = "UTF-16LE"
    val exception = intercept[IllegalArgumentException] {
      spark.read
        .options(Map("encoding" -> encoding))
        .json(testFile("test-data/utf16LE.json"))
        .count()
    }

    assert(exception.getMessage.contains(
      s"""The lineSep option must be specified for the $encoding encoding"""))
  }

  private val badJson = "\u0000\u0000\u0000A\u0001AAA"

  test("SPARK-23094: permissively read JSON file with leading nulls when multiLine is enabled") {
    withTempPath { tempDir =>
      val path = tempDir.getAbsolutePath
      Seq(badJson + """{"a":1}""").toDS().write.text(path)
      val expected = s"""${badJson}{"a":1}\n"""
      val schema = new StructType().add("a", IntegerType).add("_corrupt_record", StringType)
      val df = spark.read.format("json")
        .option("mode", "PERMISSIVE")
        .option("multiLine", true)
        .option("encoding", "UTF-8")
        .schema(schema).load(path)
      checkAnswer(df, Row(null, expected))
    }
  }

  test("SPARK-23094: permissively read JSON file with leading nulls when multiLine is disabled") {
    withTempPath { tempDir =>
      val path = tempDir.getAbsolutePath
      Seq(badJson, """{"a":1}""").toDS().write.text(path)
      val schema = new StructType().add("a", IntegerType).add("_corrupt_record", StringType)
      val df = spark.read.format("json")
        .option("mode", "PERMISSIVE")
        .option("multiLine", false)
        .option("encoding", "UTF-8")
        .schema(schema).load(path)
      checkAnswer(df, Seq(Row(1, null), Row(null, badJson)))
    }
  }

  test("SPARK-23094: permissively parse a dataset contains JSON with leading nulls") {
    checkAnswer(
      spark.read.option("mode", "PERMISSIVE").option("encoding", "UTF-8").json(Seq(badJson).toDS()),
      Row(badJson))
  }

  test("SPARK-23772 ignore column of all null values or empty array during schema inference") {
     withTempPath { tempDir =>
      val path = tempDir.getAbsolutePath

      // primitive types
      Seq(
        """{"a":null, "b":1, "c":3.0}""",
        """{"a":null, "b":null, "c":"string"}""",
        """{"a":null, "b":null, "c":null}""")
        .toDS().write.text(path)
      var df = spark.read.format("json")
        .option("dropFieldIfAllNull", true)
        .load(path)
      var expectedSchema = new StructType()
        .add("b", LongType).add("c", StringType)
      assert(df.schema === expectedSchema)
      checkAnswer(df, Row(1, "3.0") :: Row(null, "string") :: Row(null, null) :: Nil)

      // arrays
      Seq(
        """{"a":[2, 1], "b":[null, null], "c":null, "d":[[], [null]], "e":[[], null, [[]]]}""",
        """{"a":[null], "b":[null], "c":[], "d":[null, []], "e":null}""",
        """{"a":null, "b":null, "c":[], "d":null, "e":[null, [], null]}""")
        .toDS().write.mode("overwrite").text(path)
      df = spark.read.format("json")
        .option("dropFieldIfAllNull", true)
        .load(path)
      expectedSchema = new StructType()
        .add("a", ArrayType(LongType))
      assert(df.schema === expectedSchema)
      checkAnswer(df, Row(Array(2, 1)) :: Row(Array(null)) ::  Row(null) :: Nil)

      // structs
      Seq(
        """{"a":{"a1": 1, "a2":"string"}, "b":{}}""",
        """{"a":{"a1": 2, "a2":null}, "b":{"b1":[null]}}""",
        """{"a":null, "b":null}""")
        .toDS().write.mode("overwrite").text(path)
      df = spark.read.format("json")
        .option("dropFieldIfAllNull", true)
        .load(path)
      expectedSchema = new StructType()
        .add("a", StructType(StructField("a1", LongType) :: StructField("a2", StringType)
          :: Nil))
      assert(df.schema === expectedSchema)
      checkAnswer(df, Row(Row(1, "string")) :: Row(Row(2, null)) :: Row(null) :: Nil)
    }
  }

  test("SPARK-24190: restrictions for JSONOptions in read") {
    for (encoding <- Set("UTF-16", "UTF-32")) {
      val exception = intercept[IllegalArgumentException] {
        spark.read
          .option("encoding", encoding)
          .option("multiLine", false)
          .json(testFile("test-data/utf16LE.json"))
          .count()
      }
      assert(exception.getMessage.contains("encoding must not be included in the blacklist"))
    }
  }

  test("count() for malformed input") {
    def countForMalformedJSON(expected: Long, input: Seq[String]): Unit = {
      val schema = new StructType().add("a", StringType)
      val strings = spark.createDataset(input)
      val df = spark.read.schema(schema).json(strings)

      assert(df.count() == expected)
    }
    def checkCount(expected: Long): Unit = {
      val validRec = """{"a":"b"}"""
      val inputs = Seq(
        Seq("{-}", validRec),
        Seq(validRec, "?"),
        Seq("}", validRec),
        Seq(validRec, """{"a": [1, 2, 3]}"""),
        Seq("""{"a": {"a": "b"}}""", validRec)
      )
      inputs.foreach { input =>
        countForMalformedJSON(expected, input)
      }
    }

    checkCount(2)
    countForMalformedJSON(0, Seq(""))
  }

  test("SPARK-26745: count() for non-multiline input with empty lines") {
    withTempPath { tempPath =>
      val path = tempPath.getCanonicalPath
      Seq("""{ "a" : 1 }""", "", """     { "a" : 2 }""", " \t ")
        .toDS()
        .repartition(1)
        .write
        .text(path)
      assert(spark.read.json(path).count() === 2)
    }
  }

  test("SPARK-25040: empty strings should be disallowed") {
    def failedOnEmptyString(dataType: DataType): Unit = {
       val df = spark.read.schema(s"a ${dataType.catalogString}")
        .option("mode", "FAILFAST").json(Seq("""{"a":""}""").toDS)
      val errMessage = intercept[SparkException] {
        df.collect()
      }.getMessage
      assert(errMessage.contains(
        s"Failed to parse an empty string for data type ${dataType.catalogString}"))
    }

    def emptyString(dataType: DataType, expected: Any): Unit = {
      val df = spark.read.schema(s"a ${dataType.catalogString}")
        .option("mode", "FAILFAST").json(Seq("""{"a":""}""").toDS)
      checkAnswer(df, Row(expected) :: Nil)
    }

    failedOnEmptyString(BooleanType)
    failedOnEmptyString(ByteType)
    failedOnEmptyString(ShortType)
    failedOnEmptyString(IntegerType)
    failedOnEmptyString(LongType)
    failedOnEmptyString(FloatType)
    failedOnEmptyString(DoubleType)
    failedOnEmptyString(DecimalType.SYSTEM_DEFAULT)
    failedOnEmptyString(TimestampType)
    failedOnEmptyString(DateType)
    failedOnEmptyString(ArrayType(IntegerType))
    failedOnEmptyString(MapType(StringType, IntegerType, true))
    failedOnEmptyString(StructType(StructField("f1", IntegerType, true) :: Nil))

    emptyString(StringType, "")
    emptyString(BinaryType, "".getBytes(StandardCharsets.UTF_8))
  }

  test("do not produce empty files for empty partitions") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.emptyDataset[String].write.json(path)
      val files = new File(path).listFiles()
      assert(!files.exists(_.getName.endsWith("json")))
    }
  }

  test("return partial result for bad records") {
    val schema = "a double, b array<int>, c string, _corrupt_record string"
    val badRecords = Seq(
      """{"a":"-","b":[0, 1, 2],"c":"abc"}""",
      """{"a":0.1,"b":{},"c":"def"}""").toDS()
    val df = spark.read.schema(schema).json(badRecords)

    checkAnswer(
      df,
      Row(null, Array(0, 1, 2), "abc", """{"a":"-","b":[0, 1, 2],"c":"abc"}""") ::
      Row(0.1, null, "def", """{"a":0.1,"b":{},"c":"def"}""") :: Nil)
  }

  test("inferring timestamp type") {
    def schemaOf(jsons: String*): StructType = spark.read.json(jsons.toDS).schema

    assert(schemaOf(
      """{"a":"2018-12-17T10:11:12.123-01:00"}""",
      """{"a":"2018-12-16T22:23:24.123-02:00"}""") === fromDDL("a timestamp"))

    assert(schemaOf("""{"a":"2018-12-17T10:11:12.123-01:00"}""", """{"a":1}""")
      === fromDDL("a string"))
    assert(schemaOf("""{"a":"2018-12-17T10:11:12.123-01:00"}""", """{"a":"123"}""")
      === fromDDL("a string"))

    assert(schemaOf("""{"a":"2018-12-17T10:11:12.123-01:00"}""", """{"a":null}""")
      === fromDDL("a timestamp"))
    assert(schemaOf("""{"a":null}""", """{"a":"2018-12-17T10:11:12.123-01:00"}""")
      === fromDDL("a timestamp"))
  }

  test("roundtrip for timestamp type inferring") {
    val customSchema = new StructType().add("date", TimestampType)
    withTempDir { dir =>
      val timestampsWithFormatPath = s"${dir.getCanonicalPath}/timestampsWithFormat.json"
      val timestampsWithFormat = spark.read
        .option("timestampFormat", "dd/MM/yyyy HH:mm")
        .json(datesRecords)
      assert(timestampsWithFormat.schema === customSchema)

      timestampsWithFormat.write
        .format("json")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
        .save(timestampsWithFormatPath)

      val readBack = spark.read
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
        .json(timestampsWithFormatPath)

      assert(readBack.schema === customSchema)
      checkAnswer(readBack, timestampsWithFormat)
    }
  }
}
