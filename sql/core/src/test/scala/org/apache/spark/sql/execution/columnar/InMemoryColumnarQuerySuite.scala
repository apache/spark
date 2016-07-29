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

package org.apache.spark.sql.execution.columnar

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

class InMemoryColumnarQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  test("simple columnar query") {
    val plan = sqlContext.executePlan(testData.logicalPlan).executedPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().toSeq)
  }

  test("default size avoids broadcast") {
    // TODO: Improve this test when we have better statistics
    sparkContext.parallelize(1 to 10).map(i => TestData(i, i.toString))
      .toDF().registerTempTable("sizeTst")
    sqlContext.cacheTable("sizeTst")
    assert(
      sqlContext.table("sizeTst").queryExecution.analyzed.statistics.sizeInBytes >
        sqlContext.conf.autoBroadcastJoinThreshold)
  }

  test("projection") {
    val plan = sqlContext.executePlan(testData.select('value, 'key).logicalPlan).executedPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().map {
      case Row(key: Int, value: String) => value -> key
    }.map(Row.fromTuple))
  }

  test("SPARK-1436 regression: in-memory columns must be able to be accessed multiple times") {
    val plan = sqlContext.executePlan(testData.logicalPlan).executedPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().toSeq)
    checkAnswer(scan, testData.collect().toSeq)
  }

  test("SPARK-1678 regression: compression must not lose repeated values") {
    checkAnswer(
      sql("SELECT * FROM repeatedData"),
      repeatedData.collect().toSeq.map(Row.fromTuple))

    sqlContext.cacheTable("repeatedData")

    checkAnswer(
      sql("SELECT * FROM repeatedData"),
      repeatedData.collect().toSeq.map(Row.fromTuple))
  }

  test("with null values") {
    checkAnswer(
      sql("SELECT * FROM nullableRepeatedData"),
      nullableRepeatedData.collect().toSeq.map(Row.fromTuple))

    sqlContext.cacheTable("nullableRepeatedData")

    checkAnswer(
      sql("SELECT * FROM nullableRepeatedData"),
      nullableRepeatedData.collect().toSeq.map(Row.fromTuple))
  }

  test("SPARK-2729 regression: timestamp data type") {
    val timestamps = (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time")
    timestamps.registerTempTable("timestamps")

    checkAnswer(
      sql("SELECT time FROM timestamps"),
      timestamps.collect().toSeq)

    sqlContext.cacheTable("timestamps")

    checkAnswer(
      sql("SELECT time FROM timestamps"),
      timestamps.collect().toSeq)
  }

  test("SPARK-3320 regression: batched column buffer building should work with empty partitions") {
    checkAnswer(
      sql("SELECT * FROM withEmptyParts"),
      withEmptyParts.collect().toSeq.map(Row.fromTuple))

    sqlContext.cacheTable("withEmptyParts")

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
        FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
        DateType, TimestampType,
        ArrayType(IntegerType), MapType(StringType, LongType), struct)
    val fields = dataTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, true)
    }
    val allColumns = fields.map(_.name).mkString(",")
    val schema = StructType(fields)

    // Create a RDD for the schema
    val rdd =
      sparkContext.parallelize((1 to 10000), 10).map { i =>
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
          new Timestamp(i * 1000000L),
          (i to i + 10).toSeq,
          (i to i + 10).map(j => s"map_key_$j" -> (Long.MaxValue - j)).toMap,
          Row((i - 0.25).toFloat, Seq(true, false, null)))
      }
    sqlContext.createDataFrame(rdd, schema).registerTempTable("InMemoryCache_different_data_types")
    // Cache the table.
    sql("cache table InMemoryCache_different_data_types")
    // Make sure the table is indeed cached.
    sqlContext.table("InMemoryCache_different_data_types").queryExecution.executedPlan
    assert(
      sqlContext.isCached("InMemoryCache_different_data_types"),
      "InMemoryCache_different_data_types should be cached.")
    // Issue a query and check the results.
    checkAnswer(
      sql(s"SELECT DISTINCT ${allColumns} FROM InMemoryCache_different_data_types"),
      sqlContext.table("InMemoryCache_different_data_types").collect())
    sqlContext.dropTempTable("InMemoryCache_different_data_types")
  }

  test("SPARK-10422: String column in InMemoryColumnarCache needs to override clone method") {
    val df = sqlContext.range(1, 100).selectExpr("id % 10 as id")
      .rdd.map(id => Tuple1(s"str_$id")).toDF("i")
    val cached = df.cache()
    // count triggers the caching action. It should not throw.
    cached.count()

    // Make sure, the DataFrame is indeed cached.
    assert(sqlContext.cacheManager.lookupCachedData(cached).nonEmpty)

    // Check result.
    checkAnswer(
      cached,
      sqlContext.range(1, 100).selectExpr("id % 10 as id")
        .rdd.map(id => Tuple1(s"str_$id")).toDF("i")
    )

    // Drop the cache.
    cached.unpersist()
  }

  test("SPARK-10859: Predicates pushed to InMemoryColumnarTableScan are not evaluated correctly") {
    val data = sqlContext.range(10).selectExpr("id", "cast(id as string) as s")
    data.cache()
    assert(data.count() === 10)
    assert(data.filter($"s" === "3").count() === 1)
  }

  test("SPARK-14138: Generated SpecificColumnarIterator can exceed JVM size limit for cached DF") {
    val length1 = 3999
    val columnTypes1 = List.fill(length1)(IntegerType)
    val columnarIterator1 = GenerateColumnAccessor.generate(columnTypes1)

    // SPARK-16664: the limit of janino is 8117
    val length2 = 8117
    val columnTypes2 = List.fill(length2)(IntegerType)
    val columnarIterator2 = GenerateColumnAccessor.generate(columnTypes2)
  }
}
