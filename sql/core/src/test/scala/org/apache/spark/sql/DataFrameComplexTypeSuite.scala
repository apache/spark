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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.DefinedByConstructorParams
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.MapObjects
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, Decimal, DoubleType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * A test suite to test DataFrame/SQL functionalities with complex types (i.e. array, struct, map).
 */
class DataFrameComplexTypeSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("UDF on struct") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(struct($"a").as("s")).select(f($"s.a")).collect()
  }

  test("UDF on named_struct") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.selectExpr("named_struct('a', a) s").select(f($"s.a")).collect()
  }

  test("UDF on array") {
    val f = udf((a: String) => a)
    val df = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
    df.select(array($"a").as("s")).select(f($"s".getItem(0))).collect()
  }

  test("UDF on map") {
    val f = udf((a: String) => a)
    val df = Seq("a" -> 1).toDF("a", "b")
    df.select(map($"a", $"b").as("s")).select(f($"s".getItem("a"))).collect()
  }

  test("SPARK-12477 accessing null element in array field") {
    val df = sparkContext.parallelize(Seq((Seq("val1", null, "val2"),
      Seq(Some(1), None, Some(2))))).toDF("s", "i")
    val nullStringRow = df.selectExpr("s[1]").collect()(0)
    assert(nullStringRow == org.apache.spark.sql.Row(null))
    val nullIntRow = df.selectExpr("i[1]").collect()(0)
    assert(nullIntRow == org.apache.spark.sql.Row(null))
  }

  test("SPARK-15285 Generated SpecificSafeProjection.apply method grows beyond 64KB") {
    val ds100_5 = Seq(S100_5()).toDS()
    ds100_5.rdd.count()
  }

  test("SPARK-29503 nest unsafe struct inside safe array") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      val df = spark.sparkContext.parallelize(Seq(Seq(1, 2, 3))).toDF("items")

      // items: Seq[Int] => items.map { item => Seq(Struct(item)) }
      val result = df.select(
        Column(MapObjects(
          (item: Expression) => array(struct(Column(item))).expr,
          $"items".expr,
          df.schema("items").dataType.asInstanceOf[ArrayType].elementType
        )) as "items"
      ).collect()

      assert(result.length === 1)
      assert(result === Row(Seq(Seq(Row(1)), Seq(Row(2)), Seq(Row(3)))) :: Nil)
    }
  }

  test("access complex data") {
    assert(complexData.filter(complexData("a").getItem(0) === 2).count() == 1)
    if (!conf.ansiEnabled) {
      assert(complexData.filter(complexData("m").getItem("1") === 1).count() == 1)
    }
    assert(complexData.filter(complexData("s").getField("key") === 1).count() == 1)
  }

  test("SPARK-7133: Implement struct, array, and map field accessor") {
    assert(complexData.filter(complexData("a")(0) === 2).count() == 1)
    if (!conf.ansiEnabled) {
      assert(complexData.filter(complexData("m")("1") === 1).count() == 1)
    }
    assert(complexData.filter(complexData("s")("key") === 1).count() == 1)
    assert(complexData.filter(complexData("m")(complexData("s")("value")) === 1).count() == 1)
    assert(complexData.filter(complexData("a")(complexData("s")("key")) === 1).count() == 1)
  }

  test("SPARK-24313: access map with binary keys") {
    val mapWithBinaryKey = map(lit(Array[Byte](1.toByte)), lit(1))
    checkAnswer(spark.range(1).select(mapWithBinaryKey.getItem(Array[Byte](1.toByte))), Row(1))
  }

  test("SPARK-37855: IllegalStateException when transforming an array inside a nested struct") {
    def makeInput(): DataFrame = {
      val innerElement1 = Row(3, 3.12)
      val innerElement2 = Row(4, 2.1)
      val innerElement3 = Row(1, 985.2)
      val innerElement4 = Row(10, 757548.0)
      val innerElement5 = Row(1223, 0.665)

      val outerElement1 = Row(1, Row(List(innerElement1, innerElement2)))
      val outerElement2 = Row(2, Row(List(innerElement3)))
      val outerElement3 = Row(3, Row(List(innerElement4, innerElement5)))

      val data = Seq(
        Row("row1", List(outerElement1)),
        Row("row2", List(outerElement2, outerElement3))
      )

      val schema = new StructType()
        .add("name", StringType)
        .add("outer_array", ArrayType(new StructType()
          .add("id", IntegerType)
          .add("inner_array_struct", new StructType()
            .add("inner_array", ArrayType(new StructType()
              .add("id", IntegerType)
              .add("value", DoubleType)
            ))
          )
        ))

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }

    val df = makeInput().limit(2)

    val res = df.withColumn("extracted", transform(
      col("outer_array"),
      c1 => {
        struct(
          c1.getField("id").alias("outer_id"),
          transform(
            c1.getField("inner_array_struct").getField("inner_array"),
            c2 => {
              struct(
                c2.getField("value").alias("inner_value")
              )
            }
          )
        )
      }
    ))

    assert(res.collect().length == 2)
  }

  test("SPARK-39293: The accumulator of ArrayAggregate to handle complex types properly") {
    val reverse = udf((s: String) => s.reverse)

    val df = Seq(Array("abc", "def")).toDF("array")
    val testArray = df.select(
      aggregate(
        col("array"),
        array().cast("array<string>"),
        (acc, s) => concat(acc, array(reverse(s)))))
    checkAnswer(testArray, Row(Array("cba", "fed")) :: Nil)

    val testMap = df.select(
      aggregate(
        col("array"),
        map().cast("map<string, string>"),
        (acc, s) => map_concat(acc, map(s, reverse(s)))))
    checkAnswer(testMap, Row(Map("abc" -> "cba", "def" -> "fed")) :: Nil)
  }

  test("SPARK-31552: array encoder with different types") {
    // primitives
    val booleans = Array(true, false)
    checkAnswer(Seq(booleans).toDF(), Row(booleans))

    val bytes = Array(1.toByte, 2.toByte)
    checkAnswer(Seq(bytes).toDF(), Row(bytes))
    val shorts = Array(1.toShort, 2.toShort)
    checkAnswer(Seq(shorts).toDF(), Row(shorts))
    val ints = Array(1, 2)
    checkAnswer(Seq(ints).toDF(), Row(ints))
    val longs = Array(1L, 2L)
    checkAnswer(Seq(longs).toDF(), Row(longs))

    val floats = Array(1.0F, 2.0F)
    checkAnswer(Seq(floats).toDF(), Row(floats))
    val doubles = Array(1.0D, 2.0D)
    checkAnswer(Seq(doubles).toDF(), Row(doubles))

    val strings = Array("2020-04-24", "2020-04-25")
    checkAnswer(Seq(strings).toDF(), Row(strings))

    // tuples
    val decOne = Decimal(1, 38, 18)
    val decTwo = Decimal(2, 38, 18)
    val tuple1 = (1, 2.2, "3.33", decOne, Date.valueOf("2012-11-22"))
    val tuple2 = (2, 3.3, "4.44", decTwo, Date.valueOf("2022-11-22"))
    checkAnswer(Seq(Array(tuple1, tuple2)).toDF(), Seq(Seq(tuple1, tuple2)).toDF())

    // case classes
    val gbks = Array(GroupByKey(1, 2), GroupByKey(4, 5))
    checkAnswer(Seq(gbks).toDF(), Row(Array(Row(1, 2), Row(4, 5))))

    // We can move this implicit def to [[SQLImplicits]] when we eventually make fully
    // support for array encoder like Seq and Set
    // For now cases below, decimal/datetime/interval/binary/nested types, etc,
    // are not supported by array
    implicit def newArrayEncoder[T <: Array[_] : TypeTag]: Encoder[T] = ExpressionEncoder()

    // decimals
    val decSpark = Array(decOne, decTwo)
    val decScala = decSpark.map(_.toBigDecimal)
    val decJava = decSpark.map(_.toJavaBigDecimal)
    checkAnswer(Seq(decSpark).toDF(), Row(decJava))
    checkAnswer(Seq(decScala).toDF(), Row(decJava))
    checkAnswer(Seq(decJava).toDF(), Row(decJava))

    // datetimes and intervals
    val dates = strings.map(Date.valueOf)
    checkAnswer(Seq(dates).toDF(), Row(dates))
    val localDates = dates.map(d => DateTimeUtils.daysToLocalDate(DateTimeUtils.fromJavaDate(d)))
    checkAnswer(Seq(localDates).toDF(), Row(dates))

    val timestamps =
      Array(Timestamp.valueOf("2020-04-24 12:34:56"), Timestamp.valueOf("2020-04-24 11:22:33"))
    checkAnswer(Seq(timestamps).toDF(), Row(timestamps))
    val instants =
      timestamps.map(t => DateTimeUtils.microsToInstant(DateTimeUtils.fromJavaTimestamp(t)))
    checkAnswer(Seq(instants).toDF(), Row(timestamps))

    val intervals = Array(new CalendarInterval(1, 2, 3), new CalendarInterval(4, 5, 6))
    checkAnswer(Seq(intervals).toDF(), Row(intervals))

    // binary
    val bins = Array(Array(1.toByte), Array(2.toByte), Array(3.toByte), Array(4.toByte))
    checkAnswer(Seq(bins).toDF(), Row(bins))

    // nested
    val nestedIntArray = Array(Array(1), Array(2))
    checkAnswer(Seq(nestedIntArray).toDF(), Row(nestedIntArray.map(wrapIntArray)))
    val nestedDecArray = Array(decSpark)
    checkAnswer(Seq(nestedDecArray).toDF(), Row(Array(wrapRefArray(decJava))))
  }

  test("SPARK-24165: CaseWhen/If - nullability of nested types") {
    val rows = new java.util.ArrayList[Row]()
    rows.add(Row(true, ("x", 1), Seq("x", "y"), Map(0 -> "x")))
    rows.add(Row(false, (null, 2), Seq(null, "z"), Map(0 -> null)))
    val schema = StructType(Seq(
      StructField("cond", BooleanType, true),
      StructField("s", StructType(Seq(
        StructField("val1", StringType, true),
        StructField("val2", IntegerType, false)
      )), false),
      StructField("a", ArrayType(StringType, true)),
      StructField("m", MapType(IntegerType, StringType, true))
    ))

    val sourceDF = spark.createDataFrame(rows, schema)

    def structWhenDF: DataFrame = sourceDF
      .select(when($"cond",
        struct(lit("a").as("val1"), lit(10).as("val2"))).otherwise($"s") as "res")
      .select($"res".getField("val1"))
    def arrayWhenDF: DataFrame = sourceDF
      .select(when($"cond", array(lit("a"), lit("b"))).otherwise($"a") as "res")
      .select($"res".getItem(0))
    def mapWhenDF: DataFrame = sourceDF
      .select(when($"cond", map(lit(0), lit("a"))).otherwise($"m") as "res")
      .select($"res".getItem(0))

    def structIfDF: DataFrame = sourceDF
      .select(expr("if(cond, struct('a' as val1, 10 as val2), s)") as "res")
      .select($"res".getField("val1"))
    def arrayIfDF: DataFrame = sourceDF
      .select(expr("if(cond, array('a', 'b'), a)") as "res")
      .select($"res".getItem(0))
    def mapIfDF: DataFrame = sourceDF
      .select(expr("if(cond, map(0, 'a'), m)") as "res")
      .select($"res".getItem(0))

    def checkResult(): Unit = {
      checkAnswer(structWhenDF, Seq(Row("a"), Row(null)))
      checkAnswer(arrayWhenDF, Seq(Row("a"), Row(null)))
      checkAnswer(mapWhenDF, Seq(Row("a"), Row(null)))
      checkAnswer(structIfDF, Seq(Row("a"), Row(null)))
      checkAnswer(arrayIfDF, Seq(Row("a"), Row(null)))
      checkAnswer(mapIfDF, Seq(Row("a"), Row(null)))
    }

    // Test with local relation, the Project will be evaluated without codegen
    checkResult()
    // Test with cached relation, the Project will be evaluated with codegen
    sourceDF.cache()
    checkResult()
  }
}

class S100(
  val s1: String = "1", val s2: String = "2", val s3: String = "3", val s4: String = "4",
  val s5: String = "5", val s6: String = "6", val s7: String = "7", val s8: String = "8",
  val s9: String = "9", val s10: String = "10", val s11: String = "11", val s12: String = "12",
  val s13: String = "13", val s14: String = "14", val s15: String = "15", val s16: String = "16",
  val s17: String = "17", val s18: String = "18", val s19: String = "19", val s20: String = "20",
  val s21: String = "21", val s22: String = "22", val s23: String = "23", val s24: String = "24",
  val s25: String = "25", val s26: String = "26", val s27: String = "27", val s28: String = "28",
  val s29: String = "29", val s30: String = "30", val s31: String = "31", val s32: String = "32",
  val s33: String = "33", val s34: String = "34", val s35: String = "35", val s36: String = "36",
  val s37: String = "37", val s38: String = "38", val s39: String = "39", val s40: String = "40",
  val s41: String = "41", val s42: String = "42", val s43: String = "43", val s44: String = "44",
  val s45: String = "45", val s46: String = "46", val s47: String = "47", val s48: String = "48",
  val s49: String = "49", val s50: String = "50", val s51: String = "51", val s52: String = "52",
  val s53: String = "53", val s54: String = "54", val s55: String = "55", val s56: String = "56",
  val s57: String = "57", val s58: String = "58", val s59: String = "59", val s60: String = "60",
  val s61: String = "61", val s62: String = "62", val s63: String = "63", val s64: String = "64",
  val s65: String = "65", val s66: String = "66", val s67: String = "67", val s68: String = "68",
  val s69: String = "69", val s70: String = "70", val s71: String = "71", val s72: String = "72",
  val s73: String = "73", val s74: String = "74", val s75: String = "75", val s76: String = "76",
  val s77: String = "77", val s78: String = "78", val s79: String = "79", val s80: String = "80",
  val s81: String = "81", val s82: String = "82", val s83: String = "83", val s84: String = "84",
  val s85: String = "85", val s86: String = "86", val s87: String = "87", val s88: String = "88",
  val s89: String = "89", val s90: String = "90", val s91: String = "91", val s92: String = "92",
  val s93: String = "93", val s94: String = "94", val s95: String = "95", val s96: String = "96",
  val s97: String = "97", val s98: String = "98", val s99: String = "99", val s100: String = "100")
extends DefinedByConstructorParams

case class S100_5(
  s1: S100 = new S100(), s2: S100 = new S100(), s3: S100 = new S100(),
  s4: S100 = new S100(), s5: S100 = new S100())


