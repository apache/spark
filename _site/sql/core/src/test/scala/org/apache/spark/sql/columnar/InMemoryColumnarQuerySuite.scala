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

package org.apache.spark.sql.columnar

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.test.TestSQLContext.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, TestData}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

class InMemoryColumnarQuerySuite extends QueryTest {
  // Make sure the tables are loaded.
  TestData

  test("simple columnar query") {
    val plan = executePlan(testData.logicalPlan).executedPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().toSeq)
  }

  test("default size avoids broadcast") {
    // TODO: Improve this test when we have better statistics
    sparkContext.parallelize(1 to 10).map(i => TestData(i, i.toString))
      .toDF().registerTempTable("sizeTst")
    cacheTable("sizeTst")
    assert(
      table("sizeTst").queryExecution.analyzed.statistics.sizeInBytes >
        conf.autoBroadcastJoinThreshold)
  }

  test("projection") {
    val plan = executePlan(testData.select('value, 'key).logicalPlan).executedPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().map {
      case Row(key: Int, value: String) => value -> key
    }.map(Row.fromTuple))
  }

  test("SPARK-1436 regression: in-memory columns must be able to be accessed multiple times") {
    val plan = executePlan(testData.logicalPlan).executedPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().toSeq)
    checkAnswer(scan, testData.collect().toSeq)
  }

  test("SPARK-1678 regression: compression must not lose repeated values") {
    checkAnswer(
      sql("SELECT * FROM repeatedData"),
      repeatedData.collect().toSeq.map(Row.fromTuple))

    cacheTable("repeatedData")

    checkAnswer(
      sql("SELECT * FROM repeatedData"),
      repeatedData.collect().toSeq.map(Row.fromTuple))
  }

  test("with null values") {
    checkAnswer(
      sql("SELECT * FROM nullableRepeatedData"),
      nullableRepeatedData.collect().toSeq.map(Row.fromTuple))

    cacheTable("nullableRepeatedData")

    checkAnswer(
      sql("SELECT * FROM nullableRepeatedData"),
      nullableRepeatedData.collect().toSeq.map(Row.fromTuple))
  }

  test("SPARK-2729 regression: timestamp data type") {
    checkAnswer(
      sql("SELECT time FROM timestamps"),
      timestamps.collect().toSeq.map(Row.fromTuple))

    cacheTable("timestamps")

    checkAnswer(
      sql("SELECT time FROM timestamps"),
      timestamps.collect().toSeq.map(Row.fromTuple))
  }

  test("SPARK-3320 regression: batched column buffer building should work with empty partitions") {
    checkAnswer(
      sql("SELECT * FROM withEmptyParts"),
      withEmptyParts.collect().toSeq.map(Row.fromTuple))

    cacheTable("withEmptyParts")

    checkAnswer(
      sql("SELECT * FROM withEmptyParts"),
      withEmptyParts.collect().toSeq.map(Row.fromTuple))
  }

  test("SPARK-4182 Caching complex types") {
    complexData.cache().count()
    // Shouldn't throw
    complexData.count()
    complexData.unpersist()
  }

  test("decimal type") {
    // Casting is required here because ScalaReflection can't capture decimal precision information.
    val df = (1 to 10)
      .map(i => Tuple1(Decimal(i, 15, 10)))
      .toDF("dec")
      .select($"dec" cast DecimalType(15, 10))

    assert(df.schema.head.dataType === DecimalType(15, 10))

    df.cache().registerTempTable("test_fixed_decimal")
    checkAnswer(
      sql("SELECT * FROM test_fixed_decimal"),
      (1 to 10).map(i => Row(Decimal(i, 15, 10).toJavaBigDecimal)))
  }

  test("test different data types") {
    // Create the schema.
    val struct =
      StructType(
        StructField("f1", FloatType, true) ::
        StructField("f2", ArrayType(BooleanType), true) :: Nil)
    val dataTypes =
      Seq(StringType, BinaryType, NullType, BooleanType,
        ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, DecimalType.Unlimited, DecimalType(6, 5),
        DateType, TimestampType,
        ArrayType(IntegerType), MapType(StringType, LongType), struct)
    val fields = dataTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, true)
    }
    val allColumns = fields.map(_.name).mkString(",")
    val schema = StructType(fields)

    // Create a RDD for the schema
    val rdd =
      sparkContext.parallelize((1 to 100), 10).map { i =>
        Row(
          s"str${i}: test cache.",
          s"binary${i}: test cache.".getBytes("UTF-8"),
          null,
          i % 2 == 0,
          i.toByte,
          i.toShort,
          i,
          Long.MaxValue - i.toLong,
          (i + 0.25).toFloat,
          (i + 0.75),
          BigDecimal(Long.MaxValue.toString + ".12345"),
          new java.math.BigDecimal(s"${i % 9 + 1}" + ".23456"),
          new Date(i),
          new Timestamp(i),
          (1 to i).toSeq,
          (0 to i).map(j => s"map_key_$j" -> (Long.MaxValue - j)).toMap,
          Row((i - 0.25).toFloat, (1 to i).toSeq))
      }
    createDataFrame(rdd, schema).registerTempTable("InMemoryCache_different_data_types")
    // Cache the table.
    sql("cache table InMemoryCache_different_data_types")
    // Make sure the table is indeed cached.
    val tableScan = table("InMemoryCache_different_data_types").queryExecution.executedPlan
    assert(
      isCached("InMemoryCache_different_data_types"),
      "InMemoryCache_different_data_types should be cached.")
    // Issue a query and check the results.
    checkAnswer(
      sql(s"SELECT DISTINCT ${allColumns} FROM InMemoryCache_different_data_types"),
      table("InMemoryCache_different_data_types").collect())
    dropTempTable("InMemoryCache_different_data_types")
  }
}
