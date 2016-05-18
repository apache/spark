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

import java.io.{File, StringWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.core.JsonFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.json.InferSchema.compatibleType
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
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
    def enforceCorrectType(value: Any, dataType: DataType): Any = {
      val writer = new StringWriter()
      Utils.tryWithResource(factory.createGenerator(writer)) { generator =>
        generator.writeObject(value)
        generator.flush()
      }

      Utils.tryWithResource(factory.createParser(writer.toString)) { parser =>
        parser.nextToken()
        JacksonParser.convertRootField(factory, parser, dataType)
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
    checkTypePromotion(DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(strTime)),
        enforceCorrectType(strTime, TimestampType))

    val strDate = "2014-10-15"
    checkTypePromotion(
      DateTimeUtils.fromJavaDate(Date.valueOf(strDate)), enforceCorrectType(strDate, DateType))

    val ISO8601Time1 = "1970-01-01T01:00:01.0Z"
    checkTypePromotion(DateTimeUtils.fromJavaTimestamp(new Timestamp(3601000)),
        enforceCorrectType(ISO8601Time1, TimestampType))
    checkTypePromotion(DateTimeUtils.millisToDays(3601000),
      enforceCorrectType(ISO8601Time1, DateType))
    val ISO8601Time2 = "1970-01-01T02:00:01-01:00"
    checkTypePromotion(DateTimeUtils.fromJavaTimestamp(new Timestamp(10801000)),
        enforceCorrectType(ISO8601Time2, TimestampType))
    checkTypePromotion(DateTimeUtils.millisToDays(10801000),
      enforceCorrectType(ISO8601Time2, DateType))
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
      sql("select num_str + 1.2 from jsonTable where num_str > 14"),
      Row(BigDecimal("92233720368547758071.2"))
    )

    // Number and String conflict: resolve the type as number in this query.
    checkAnswer(
      sql("select num_str + 1.2 from jsonTable where num_str >= 92233720368547758060"),
      Row(new java.math.BigDecimal("92233720368547758071.2"))
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
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)
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
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)
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
    val mixedIntegerAndDoubleRecords = sparkContext.parallelize(
      """{"a": 3, "b": 1.1}""" ::
      s"""{"a": 3.1, "b": 0.${"0" * 38}1}""" :: Nil)
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
    checkAnswer(jsonDF, Row(1.0E-39D, BigDecimal(0.01)))

    val mergedJsonDF = spark.read
      .option("prefersDecimal", "true")
      .json(floatingValueRecords ++ bigIntegerRecords)

    val expectedMergedSchema = StructType(
      StructField("a", DoubleType, true) ::
      StructField("b", DecimalType(22, 2), true):: Nil)

    assert(expectedMergedSchema === mergedJsonDF.schema)
    checkAnswer(
      mergedJsonDF,
      Row(1.0E-39D, BigDecimal(0.01)) ::
      Row(1.0E38D, BigDecimal("92233720368547758070")) :: Nil
    )
  }

  test("Loading a JSON dataset from a text file with SQL") {
    val dir = Utils.createTempDir()
    dir.delete()
    val path = dir.getCanonicalPath
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
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)

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

    checkAnswer(
      sql("select `map`['c'] from jsonWithSimpleMap"),
      Row(null) ::
      Row(null) ::
      Row(3) ::
      Row(1) ::
      Row(null) :: Nil
    )

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
    val schema = StructType(
        StructField("a", StringType, true) :: Nil)
    // `FAILFAST` mode should throw an exception for corrupt records.
    val exceptionOne = intercept[SparkException] {
      spark.read
        .option("mode", "FAILFAST")
        .json(corruptRecords)
        .collect()
    }
    assert(exceptionOne.getMessage.contains("Malformed line in FAILFAST mode: {"))

    val exceptionTwo = intercept[SparkException] {
      spark.read
        .option("mode", "FAILFAST")
        .schema(schema)
        .json(corruptRecords)
        .collect()
    }
    assert(exceptionTwo.getMessage.contains("Malformed line in FAILFAST mode: {"))
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

  test("Corrupt records: PERMISSIVE mode") {
    // Test if we can query corrupt records.
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      withTempTable("jsonTable") {
        val jsonDF = spark.read.json(corruptRecords)
        jsonDF.createOrReplaceTempView("jsonTable")
        val schema = StructType(
          StructField("_unparsed", StringType, true) ::
          StructField("a", StringType, true) ::
          StructField("b", StringType, true) ::
          StructField("c", StringType, true) :: Nil)

        assert(schema === jsonDF.schema)

        // In HiveContext, backticks should be used to access columns starting with a underscore.
        checkAnswer(
          sql(
            """
              |SELECT a, b, c, _unparsed
              |FROM jsonTable
            """.stripMargin),
          Row(null, null, null, "{") ::
            Row(null, null, null, """{"a":1, b:2}""") ::
            Row(null, null, null, """{"a":{, b:3}""") ::
            Row("str_a_4", "str_b_4", "str_c_4", null) ::
            Row(null, null, null, "]") :: Nil
        )

        checkAnswer(
          sql(
            """
              |SELECT a, b, c
              |FROM jsonTable
              |WHERE _unparsed IS NULL
            """.stripMargin),
          Row("str_a_4", "str_b_4", "str_c_4")
        )

        checkAnswer(
          sql(
            """
              |SELECT _unparsed
              |FROM jsonTable
              |WHERE _unparsed IS NOT NULL
            """.stripMargin),
          Row("{") ::
            Row("""{"a":1, b:2}""") ::
            Row("""{"a":{, b:3}""") ::
            Row("]") :: Nil
        )
      }
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
    val primTable = spark.read.json(jsonDF.toJSON.rdd)
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
    val compTable = spark.read.json(complexJsonDF.toJSON.rdd)
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
        className = classOf[DefaultSource].getCanonicalName,
        options = Map("path" -> path)).resolveRelation()

      val d2 = DataSource(
        spark,
        userSpecifiedSchema = None,
        partitionColumns = Array.empty[String],
        bucketSpec = None,
        className = classOf[DefaultSource].getCanonicalName,
        options = Map("path" -> path)).resolveRelation()
      assert(d1 === d2)
    })
  }

  test("SPARK-6245 JsonRDD.inferSchema on empty RDD") {
    // This is really a test that it doesn't throw an exception
    val emptySchema = InferSchema.infer(empty, "", new JSONOptions(Map()))
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
    val emptySchema = InferSchema.infer(emptyRecords, "", new JSONOptions(Map()))
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

  test("backward compatibility") {
    // This test we make sure our JSON support can read JSON data generated by previous version
    // of Spark generated through toJSON method and JSON data source.
    // The data is generated by the following program.
    // Here are a few notes:
    //  - Spark 1.5.0 cannot save timestamp data. So, we manually added timestamp field (col13)
    //      in the JSON object.
    //  - For Spark before 1.5.1, we do not generate UDTs. So, we manually added the UDT value to
    //      JSON objects generated by those Spark versions (col17).
    //  - If the type is NullType, we do not write data out.

    // Create the schema.
    val struct =
      StructType(
        StructField("f1", FloatType, true) ::
          StructField("f2", ArrayType(BooleanType), true) :: Nil)

    val dataTypes =
      Seq(
        StringType, BinaryType, NullType, BooleanType,
        ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
        DateType, TimestampType,
        ArrayType(IntegerType), MapType(StringType, LongType), struct,
        new UDT.MyDenseVectorUDT())
    val fields = dataTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, nullable = true)
    }
    val schema = StructType(fields)

    val constantValues =
      Seq(
        "a string in binary".getBytes(StandardCharsets.UTF_8),
        null,
        true,
        1.toByte,
        2.toShort,
        3,
        Long.MaxValue,
        0.25.toFloat,
        0.75,
        new java.math.BigDecimal(s"1234.23456"),
        new java.math.BigDecimal(s"1.23456"),
        java.sql.Date.valueOf("2015-01-01"),
        java.sql.Timestamp.valueOf("2015-01-01 23:50:59.123"),
        Seq(2, 3, 4),
        Map("a string" -> 2000L),
        Row(4.75.toFloat, Seq(false, true)),
        new UDT.MyDenseVector(Array(0.25, 2.25, 4.25)))
    val data =
      Row.fromSeq(Seq("Spark " + spark.sparkContext.version) ++ constantValues) :: Nil

    // Data generated by previous versions.
    // scalastyle:off
    val existingJSONData =
      """{"col0":"Spark 1.2.2","col1":"YSBzdHJpbmcgaW4gYmluYXJ5","col3":true,"col4":1,"col5":2,"col6":3,"col7":9223372036854775807,"col8":0.25,"col9":0.75,"col10":1234.23456,"col11":1.23456,"col12":"2015-01-01","col13":"2015-01-01 23:50:59.123","col14":[2,3,4],"col15":{"a string":2000},"col16":{"f1":4.75,"f2":[false,true]},"col17":[0.25,2.25,4.25]}""" ::
      """{"col0":"Spark 1.3.1","col1":"YSBzdHJpbmcgaW4gYmluYXJ5","col3":true,"col4":1,"col5":2,"col6":3,"col7":9223372036854775807,"col8":0.25,"col9":0.75,"col10":1234.23456,"col11":1.23456,"col12":"2015-01-01","col13":"2015-01-01 23:50:59.123","col14":[2,3,4],"col15":{"a string":2000},"col16":{"f1":4.75,"f2":[false,true]},"col17":[0.25,2.25,4.25]}""" ::
      """{"col0":"Spark 1.3.1","col1":"YSBzdHJpbmcgaW4gYmluYXJ5","col3":true,"col4":1,"col5":2,"col6":3,"col7":9223372036854775807,"col8":0.25,"col9":0.75,"col10":1234.23456,"col11":1.23456,"col12":"2015-01-01","col13":"2015-01-01 23:50:59.123","col14":[2,3,4],"col15":{"a string":2000},"col16":{"f1":4.75,"f2":[false,true]},"col17":[0.25,2.25,4.25]}""" ::
      """{"col0":"Spark 1.4.1","col1":"YSBzdHJpbmcgaW4gYmluYXJ5","col3":true,"col4":1,"col5":2,"col6":3,"col7":9223372036854775807,"col8":0.25,"col9":0.75,"col10":1234.23456,"col11":1.23456,"col12":"2015-01-01","col13":"2015-01-01 23:50:59.123","col14":[2,3,4],"col15":{"a string":2000},"col16":{"f1":4.75,"f2":[false,true]},"col17":[0.25,2.25,4.25]}""" ::
      """{"col0":"Spark 1.4.1","col1":"YSBzdHJpbmcgaW4gYmluYXJ5","col3":true,"col4":1,"col5":2,"col6":3,"col7":9223372036854775807,"col8":0.25,"col9":0.75,"col10":1234.23456,"col11":1.23456,"col12":"2015-01-01","col13":"2015-01-01 23:50:59.123","col14":[2,3,4],"col15":{"a string":2000},"col16":{"f1":4.75,"f2":[false,true]},"col17":[0.25,2.25,4.25]}""" ::
      """{"col0":"Spark 1.5.0","col1":"YSBzdHJpbmcgaW4gYmluYXJ5","col3":true,"col4":1,"col5":2,"col6":3,"col7":9223372036854775807,"col8":0.25,"col9":0.75,"col10":1234.23456,"col11":1.23456,"col12":"2015-01-01","col13":"2015-01-01 23:50:59.123","col14":[2,3,4],"col15":{"a string":2000},"col16":{"f1":4.75,"f2":[false,true]},"col17":[0.25,2.25,4.25]}""" ::
      """{"col0":"Spark 1.5.0","col1":"YSBzdHJpbmcgaW4gYmluYXJ5","col3":true,"col4":1,"col5":2,"col6":3,"col7":9223372036854775807,"col8":0.25,"col9":0.75,"col10":1234.23456,"col11":1.23456,"col12":"16436","col13":"2015-01-01 23:50:59.123","col14":[2,3,4],"col15":{"a string":2000},"col16":{"f1":4.75,"f2":[false,true]},"col17":[0.25,2.25,4.25]}""" :: Nil
    // scalastyle:on

    // Generate data for the current version.
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
    withTempPath { path =>
      df.write.format("json").mode("overwrite").save(path.getCanonicalPath)

      // df.toJSON will convert internal rows to external rows first and then generate
      // JSON objects. While, df.write.format("json") will write internal rows directly.
      val allJSON =
        existingJSONData ++
          df.toJSON.collect() ++
          sparkContext.textFile(path.getCanonicalPath).collect()

      Utils.deleteRecursively(path)
      sparkContext.parallelize(allJSON, 1).saveAsTextFile(path.getCanonicalPath)

      // Read data back with the schema specified.
      val col0Values =
        Seq(
          "Spark 1.2.2",
          "Spark 1.3.1",
          "Spark 1.3.1",
          "Spark 1.4.1",
          "Spark 1.4.1",
          "Spark 1.5.0",
          "Spark 1.5.0",
          "Spark " + spark.sparkContext.version,
          "Spark " + spark.sparkContext.version)
      val expectedResult = col0Values.map { v =>
        Row.fromSeq(Seq(v) ++ constantValues)
      }
      checkAnswer(
        spark.read.format("json").schema(schema).load(path.getCanonicalPath),
        expectedResult
      )
    }
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
      withTempTable("jsonTable") {
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
      arrayAndStructRecords.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)

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
      primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)

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
      primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)

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
    withTempTable("jsonTable") {
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
    val rdd = spark.sparkContext.makeRDD(Seq(json))
    val df = spark.read.json(rdd)
    assert(df.schema.size === 2)
    df.collect()
  }
}
