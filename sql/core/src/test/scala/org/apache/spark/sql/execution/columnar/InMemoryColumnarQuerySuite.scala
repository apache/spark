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

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._

class InMemoryColumnarQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  private def cachePrimitiveTest(data: DataFrame, dataType: String) {
    data.createOrReplaceTempView(s"testData$dataType")
    val storageLevel = MEMORY_ONLY
    val plan = spark.sessionState.executePlan(data.logicalPlan).sparkPlan
    val inMemoryRelation = InMemoryRelation(useCompression = true, 5, storageLevel, plan, None)

    assert(inMemoryRelation.cachedColumnBuffers.getStorageLevel == storageLevel)
    inMemoryRelation.cachedColumnBuffers.collect().head match {
      case _: CachedBatch =>
      case other => fail(s"Unexpected cached batch type: ${other.getClass.getName}")
    }
    checkAnswer(inMemoryRelation, data.collect().toSeq)
  }

  private def testPrimitiveType(nullability: Boolean): Unit = {
    val dataTypes = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DateType, TimestampType, DecimalType(25, 5), DecimalType(6, 5))
    val schema = StructType(dataTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, nullability)
    })
    val rdd = spark.sparkContext.parallelize((1 to 10).map(i => Row(
      if (nullability && i % 3 == 0) null else if (i % 2 == 0) true else false,
      if (nullability && i % 3 == 0) null else i.toByte,
      if (nullability && i % 3 == 0) null else i.toShort,
      if (nullability && i % 3 == 0) null else i.toInt,
      if (nullability && i % 3 == 0) null else i.toLong,
      if (nullability && i % 3 == 0) null else (i + 0.25).toFloat,
      if (nullability && i % 3 == 0) null else (i + 0.75).toDouble,
      if (nullability && i % 3 == 0) null else new Date(i),
      if (nullability && i % 3 == 0) null else new Timestamp(i * 1000000L),
      if (nullability && i % 3 == 0) null else BigDecimal(Long.MaxValue.toString + ".12345"),
      if (nullability && i % 3 == 0) null
      else new java.math.BigDecimal(s"${i % 9 + 1}" + ".23456")
    )))
    cachePrimitiveTest(spark.createDataFrame(rdd, schema), "primitivesDateTimeStamp")
  }

  private def tesNonPrimitiveType(nullability: Boolean): Unit = {
    val struct = StructType(StructField("f1", FloatType, false) ::
      StructField("f2", ArrayType(BooleanType), true) :: Nil)
    val schema = StructType(Seq(
      StructField("col0", StringType, nullability),
      StructField("col1", ArrayType(IntegerType), nullability),
      StructField("col2", ArrayType(ArrayType(IntegerType)), nullability),
      StructField("col3", MapType(StringType, IntegerType), nullability),
      StructField("col4", struct, nullability)
    ))
    val rdd = spark.sparkContext.parallelize((1 to 10).map(i => Row(
      if (nullability && i % 3 == 0) null else s"str${i}: test cache.",
      if (nullability && i % 3 == 0) null else (i * 100 to i * 100 + i).toArray,
      if (nullability && i % 3 == 0) null
      else Array(Array(i, i + 1), Array(i * 100 + 1, i * 100, i * 100 + 2)),
      if (nullability && i % 3 == 0) null else (i to i + i).map(j => s"key$j" -> j).toMap,
      if (nullability && i % 3 == 0) null else Row((i + 0.25).toFloat, Seq(true, false, null))
    )))
    cachePrimitiveTest(spark.createDataFrame(rdd, schema), "StringArrayMapStruct")
  }

  test("primitive type with nullability:true") {
    testPrimitiveType(true)
  }

  test("primitive type with nullability:false") {
    testPrimitiveType(false)
  }

  test("non-primitive type with nullability:true") {
    val schemaNull = StructType(Seq(StructField("col", NullType, true)))
    val rddNull = spark.sparkContext.parallelize((1 to 10).map(i => Row(null)))
    cachePrimitiveTest(spark.createDataFrame(rddNull, schemaNull), "Null")

    tesNonPrimitiveType(true)
  }

  test("non-primitive type with nullability:false") {
      tesNonPrimitiveType(false)
  }

  test("simple columnar query") {
    val plan = spark.sessionState.executePlan(testData.logicalPlan).sparkPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().toSeq)
  }

  test("default size avoids broadcast") {
    // TODO: Improve this test when we have better statistics
    sparkContext.parallelize(1 to 10).map(i => TestData(i, i.toString))
      .toDF().createOrReplaceTempView("sizeTst")
    spark.catalog.cacheTable("sizeTst")
    assert(
      spark.table("sizeTst").queryExecution.analyzed.stats(sqlConf).sizeInBytes >
        spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD))
  }

  test("projection") {
    val plan = spark.sessionState.executePlan(testData.select('value, 'key).logicalPlan).sparkPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().map {
      case Row(key: Int, value: String) => value -> key
    }.map(Row.fromTuple))
  }

  test("access only some column of the all of columns") {
    val df = spark.range(1, 100).map(i => (i, (i + 1).toFloat)).toDF("i", "f")
    df.cache
    df.count  // forced to build cache
    assert(df.filter("f <= 10.0").count == 9)
  }

  test("SPARK-1436 regression: in-memory columns must be able to be accessed multiple times") {
    val plan = spark.sessionState.executePlan(testData.logicalPlan).sparkPlan
    val scan = InMemoryRelation(useCompression = true, 5, MEMORY_ONLY, plan, None)

    checkAnswer(scan, testData.collect().toSeq)
    checkAnswer(scan, testData.collect().toSeq)
  }

  test("SPARK-1678 regression: compression must not lose repeated values") {
    checkAnswer(
      sql("SELECT * FROM repeatedData"),
      repeatedData.collect().toSeq.map(Row.fromTuple))

    spark.catalog.cacheTable("repeatedData")

    checkAnswer(
      sql("SELECT * FROM repeatedData"),
      repeatedData.collect().toSeq.map(Row.fromTuple))
  }

  test("with null values") {
    checkAnswer(
      sql("SELECT * FROM nullableRepeatedData"),
      nullableRepeatedData.collect().toSeq.map(Row.fromTuple))

    spark.catalog.cacheTable("nullableRepeatedData")

    checkAnswer(
      sql("SELECT * FROM nullableRepeatedData"),
      nullableRepeatedData.collect().toSeq.map(Row.fromTuple))
  }

  test("SPARK-2729 regression: timestamp data type") {
    val timestamps = (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time")
    timestamps.createOrReplaceTempView("timestamps")

    checkAnswer(
      sql("SELECT time FROM timestamps"),
      timestamps.collect().toSeq)

    spark.catalog.cacheTable("timestamps")

    checkAnswer(
      sql("SELECT time FROM timestamps"),
      timestamps.collect().toSeq)
  }

  test("SPARK-3320 regression: batched column buffer building should work with empty partitions") {
    checkAnswer(
      sql("SELECT * FROM withEmptyParts"),
      withEmptyParts.collect().toSeq.map(Row.fromTuple))

    spark.catalog.cacheTable("withEmptyParts")

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
      .map(i => Tuple1(Decimal(i, 15, 10).toJavaBigDecimal))
      .toDF("dec")
      .select($"dec" cast DecimalType(15, 10))

    assert(df.schema.head.dataType === DecimalType(15, 10))

    df.cache().createOrReplaceTempView("test_fixed_decimal")
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
        DateType, TimestampType, ArrayType(IntegerType), struct)
    val fields = dataTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, true)
    }
    val allColumns = fields.map(_.name).mkString(",")
    val schema = StructType(fields)

    // Create an RDD for the schema
    val rdd =
      sparkContext.parallelize(1 to 10000, 10).map { i =>
        Row(
          s"str$i: test cache.",
          s"binary$i: test cache.".getBytes(StandardCharsets.UTF_8),
          null,
          i % 2 == 0,
          i.toByte,
          i.toShort,
          i,
          Long.MaxValue - i.toLong,
          (i + 0.25).toFloat,
          i + 0.75,
          BigDecimal(Long.MaxValue.toString + ".12345"),
          new java.math.BigDecimal(s"${i % 9 + 1}" + ".23456"),
          new Date(i),
          new Timestamp(i * 1000000L),
          i to i + 10,
          Row((i - 0.25).toFloat, Seq(true, false, null)))
      }
    spark.createDataFrame(rdd, schema).createOrReplaceTempView("InMemoryCache_different_data_types")
    // Cache the table.
    sql("cache table InMemoryCache_different_data_types")
    // Make sure the table is indeed cached.
    spark.table("InMemoryCache_different_data_types").queryExecution.executedPlan
    assert(
      spark.catalog.isCached("InMemoryCache_different_data_types"),
      "InMemoryCache_different_data_types should be cached.")
    // Issue a query and check the results.
    checkAnswer(
      sql(s"SELECT DISTINCT ${allColumns} FROM InMemoryCache_different_data_types"),
      spark.table("InMemoryCache_different_data_types").collect())
    spark.catalog.dropTempView("InMemoryCache_different_data_types")
  }

  test("SPARK-10422: String column in InMemoryColumnarCache needs to override clone method") {
    val df = spark.range(1, 100).selectExpr("id % 10 as id")
      .rdd.map(id => Tuple1(s"str_$id")).toDF("i")
    val cached = df.cache()
    // count triggers the caching action. It should not throw.
    cached.count()

    // Make sure, the DataFrame is indeed cached.
    assert(spark.sharedState.cacheManager.lookupCachedData(cached).nonEmpty)

    // Check result.
    checkAnswer(
      cached,
      spark.range(1, 100).selectExpr("id % 10 as id")
        .rdd.map(id => Tuple1(s"str_$id")).toDF("i")
    )

    // Drop the cache.
    cached.unpersist()
  }

  test("SPARK-10859: Predicates pushed to InMemoryColumnarTableScan are not evaluated correctly") {
    val data = spark.range(10).selectExpr("id", "cast(id as string) as s")
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

  test("SPARK-17549: cached table size should be correctly calculated") {
    val data = spark.sparkContext.parallelize(1 to 10, 5).toDF()
    val plan = spark.sessionState.executePlan(data.logicalPlan).sparkPlan
    val cached = InMemoryRelation(true, 5, MEMORY_ONLY, plan, None)

    // Materialize the data.
    val expectedAnswer = data.collect()
    checkAnswer(cached, expectedAnswer)

    // Check that the right size was calculated.
    assert(cached.batchStats.value === expectedAnswer.size * INT.defaultSize)
  }

  test("access primitive-type columns in CachedBatch without whole stage codegen") {
    // whole stage codegen is not applied to a row with more than WHOLESTAGE_MAX_NUM_FIELDS fields
    withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "2") {
      val data = Seq(null, true, 1.toByte, 3.toShort, 7, 15.toLong,
        31.25.toFloat, 63.75, new Date(127), new Timestamp(255000000L), null)
      val dataTypes = Seq(NullType, BooleanType, ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, DateType, TimestampType, IntegerType)
      val schemas = dataTypes.zipWithIndex.map { case (dataType, index) =>
        StructField(s"col$index", dataType, true)
      }
      val rdd = sparkContext.makeRDD(Seq(Row.fromSeq(data)))
      val df = spark.createDataFrame(rdd, StructType(schemas))
      val row = df.persist.take(1).apply(0)
      checkAnswer(df, row)
    }
  }

  test("access decimal/string-type columns in CachedBatch without whole stage codegen") {
    withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "2") {
      val data = Seq(BigDecimal(Long.MaxValue.toString + ".12345"),
        new java.math.BigDecimal("1234567890.12345"),
        new java.math.BigDecimal("1.23456"),
        "test123"
      )
      val schemas = Seq(
        StructField("col0", DecimalType(25, 5), true),
        StructField("col1", DecimalType(15, 5), true),
        StructField("col2", DecimalType(6, 5), true),
        StructField("col3", StringType, true)
      )
      val rdd = sparkContext.makeRDD(Seq(Row.fromSeq(data)))
      val df = spark.createDataFrame(rdd, StructType(schemas))
      val row = df.persist.take(1).apply(0)
      checkAnswer(df, row)
    }
  }

  test("access non-primitive-type columns in CachedBatch without whole stage codegen") {
    withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "2") {
      val data = Seq((1 to 10).toArray,
        Array(Array(10, 11), Array(100, 111, 123)),
        Map("key1" -> 111, "key2" -> 222),
        Row(1.25.toFloat, Seq(true, false, null))
      )
      val struct = StructType(StructField("f1", FloatType, false) ::
        StructField("f2", ArrayType(BooleanType), true) :: Nil)
      val schemas = Seq(
        StructField("col0", ArrayType(IntegerType), true),
        StructField("col1", ArrayType(ArrayType(IntegerType)), true),
        StructField("col2", MapType(StringType, IntegerType), true),
        StructField("col3", struct, true)
      )
      val rdd = sparkContext.makeRDD(Seq(Row.fromSeq(data)))
      val df = spark.createDataFrame(rdd, StructType(schemas))
      val row = df.persist.take(1).apply(0)
      checkAnswer(df, row)
    }
  }

  test("InMemoryTableScanExec should return correct output ordering and partitioning") {
    val df1 = Seq((0, 0), (1, 1)).toDF
      .repartition(col("_1")).sortWithinPartitions(col("_1")).persist
    val df2 = Seq((0, 0), (1, 1)).toDF
      .repartition(col("_1")).sortWithinPartitions(col("_1")).persist

    // Because two cached dataframes have the same logical plan, this is a self-join actually.
    // So we force one of in-memory relation to alias its output. Then we can test if original and
    // aliased in-memory relations have correct ordering and partitioning.
    val joined = df1.joinWith(df2, df1("_1") === df2("_1"))

    val inMemoryScans = joined.queryExecution.executedPlan.collect {
      case m: InMemoryTableScanExec => m
    }
    inMemoryScans.foreach { inMemoryScan =>
      val sortedAttrs = AttributeSet(inMemoryScan.outputOrdering.flatMap(_.references))
      assert(sortedAttrs.subsetOf(inMemoryScan.outputSet))

      val partitionedAttrs =
        inMemoryScan.outputPartitioning.asInstanceOf[HashPartitioning].references
      assert(partitionedAttrs.subsetOf(inMemoryScan.outputSet))
    }
  }

  test("SPARK-20356: pruned InMemoryTableScanExec should have correct ordering and partitioning") {
    withSQLConf("spark.sql.shuffle.partitions" -> "200") {
      val df1 = Seq(("a", 1), ("b", 1), ("c", 2)).toDF("item", "group")
      val df2 = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("item", "id")
      val df3 = df1.join(df2, Seq("item")).select($"id", $"group".as("item")).distinct()

      df3.unpersist()
      val agg_without_cache = df3.groupBy($"item").count()

      df3.cache()
      val agg_with_cache = df3.groupBy($"item").count()
      checkAnswer(agg_without_cache, agg_with_cache)
    }
  }
}
