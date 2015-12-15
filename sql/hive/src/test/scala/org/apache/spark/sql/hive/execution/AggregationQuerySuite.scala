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

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.aggregate.{MyDoubleAvg, MyDoubleSum}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class ScalaAggregateFunction(schema: StructType) extends UserDefinedAggregateFunction {

  def inputSchema: StructType = schema

  def bufferSchema: StructType = schema

  def dataType: DataType = schema

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    (0 until schema.length).foreach { i =>
      buffer.update(i, null)
    }
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0) && input.getInt(0) == 50) {
      (0 until schema.length).foreach { i =>
        buffer.update(i, input.get(i))
      }
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(0) && buffer2.getInt(0) == 50) {
      (0 until schema.length).foreach { i =>
        buffer1.update(i, buffer2.get(i))
      }
    }
  }

  def evaluate(buffer: Row): Any = {
    Row.fromSeq(buffer.toSeq)
  }
}

class ScalaAggregateFunctionWithoutInputSchema extends UserDefinedAggregateFunction {

  def inputSchema: StructType = StructType(Nil)

  def bufferSchema: StructType = StructType(StructField("value", LongType) :: Nil)

  def dataType: DataType = LongType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0L)
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, input.getAs[Seq[Row]](0).map(_.getAs[Int]("v")).sum + buffer.getLong(0))
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
  }

  def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}

class LongProductSum extends UserDefinedAggregateFunction {
  def inputSchema: StructType = new StructType()
    .add("a", LongType)
    .add("b", LongType)

  def bufferSchema: StructType = new StructType()
    .add("product", LongType)

  def dataType: DataType = LongType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!(input.isNullAt(0) || input.isNullAt(1))) {
      buffer(0) = buffer.getLong(0) + input.getLong(0) * input.getLong(1)
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  def evaluate(buffer: Row): Any =
    buffer.getLong(0)
}

abstract class AggregationQuerySuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  override def beforeAll(): Unit = {
    val data1 = Seq[(Integer, Integer)](
      (1, 10),
      (null, -60),
      (1, 20),
      (1, 30),
      (2, 0),
      (null, -10),
      (2, -1),
      (2, null),
      (2, null),
      (null, 100),
      (3, null),
      (null, null),
      (3, null)).toDF("key", "value")
    data1.write.saveAsTable("agg1")

    val data2 = Seq[(Integer, Integer, Integer)](
      (1, 10, -10),
      (null, -60, 60),
      (1, 30, -30),
      (1, 30, 30),
      (2, 1, 1),
      (null, -10, 10),
      (2, -1, null),
      (2, 1, 1),
      (2, null, 1),
      (null, 100, -10),
      (3, null, 3),
      (null, null, null),
      (3, null, null)).toDF("key", "value1", "value2")
    data2.write.saveAsTable("agg2")

    val data3 = Seq[(Seq[Integer], Integer, Integer)](
      (Seq[Integer](1, 1), 10, -10),
      (Seq[Integer](null), -60, 60),
      (Seq[Integer](1, 1), 30, -30),
      (Seq[Integer](1), 30, 30),
      (Seq[Integer](2), 1, 1),
      (null, -10, 10),
      (Seq[Integer](2, 3), -1, null),
      (Seq[Integer](2, 3), 1, 1),
      (Seq[Integer](2, 3, 4), null, 1),
      (Seq[Integer](null), 100, -10),
      (Seq[Integer](3), null, 3),
      (null, null, null),
      (Seq[Integer](3), null, null)).toDF("key", "value1", "value2")
    data3.write.saveAsTable("agg3")

    val emptyDF = sqlContext.createDataFrame(
      sparkContext.emptyRDD[Row],
      StructType(StructField("key", StringType) :: StructField("value", IntegerType) :: Nil))
    emptyDF.registerTempTable("emptyTable")

    // Register UDAFs
    sqlContext.udf.register("mydoublesum", new MyDoubleSum)
    sqlContext.udf.register("mydoubleavg", new MyDoubleAvg)
    sqlContext.udf.register("longProductSum", new LongProductSum)
  }

  override def afterAll(): Unit = {
    sqlContext.sql("DROP TABLE IF EXISTS agg1")
    sqlContext.sql("DROP TABLE IF EXISTS agg2")
    sqlContext.sql("DROP TABLE IF EXISTS agg3")
    sqlContext.dropTempTable("emptyTable")
  }

  test("empty table") {
    // If there is no GROUP BY clause and the table is empty, we will generate a single row.
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  AVG(value),
          |  COUNT(*),
          |  COUNT(key),
          |  COUNT(value),
          |  FIRST(key),
          |  LAST(value),
          |  MAX(key),
          |  MIN(value),
          |  SUM(key)
          |FROM emptyTable
        """.stripMargin),
      Row(null, 0, 0, 0, null, null, null, null, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  AVG(value),
          |  COUNT(*),
          |  COUNT(key),
          |  COUNT(value),
          |  FIRST(key),
          |  LAST(value),
          |  MAX(key),
          |  MIN(value),
          |  SUM(key),
          |  COUNT(DISTINCT value)
          |FROM emptyTable
        """.stripMargin),
      Row(null, 0, 0, 0, null, null, null, null, null, 0) :: Nil)

    // If there is a GROUP BY clause and the table is empty, there is no output.
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  AVG(value),
          |  COUNT(*),
          |  COUNT(value),
          |  FIRST(value),
          |  LAST(value),
          |  MAX(value),
          |  MIN(value),
          |  SUM(value),
          |  COUNT(DISTINCT value)
          |FROM emptyTable
          |GROUP BY key
        """.stripMargin),
      Nil)
  }

  test("null literal") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  AVG(null),
          |  COUNT(null),
          |  FIRST(null),
          |  LAST(null),
          |  MAX(null),
          |  MIN(null),
          |  SUM(null)
        """.stripMargin),
      Row(null, 0, null, null, null, null, null) :: Nil)
  }

  test("only do grouping") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT key
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT DISTINCT value1, key
          |FROM agg2
        """.stripMargin),
      Row(10, 1) ::
        Row(-60, null) ::
        Row(30, 1) ::
        Row(1, 2) ::
        Row(-10, null) ::
        Row(-1, 2) ::
        Row(null, 2) ::
        Row(100, null) ::
        Row(null, 3) ::
        Row(null, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT value1, key
          |FROM agg2
          |GROUP BY key, value1
        """.stripMargin),
      Row(10, 1) ::
        Row(-60, null) ::
        Row(30, 1) ::
        Row(1, 2) ::
        Row(-10, null) ::
        Row(-1, 2) ::
        Row(null, 2) ::
        Row(100, null) ::
        Row(null, 3) ::
        Row(null, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT DISTINCT key
          |FROM agg3
        """.stripMargin),
      Row(Seq[Integer](1, 1)) ::
        Row(Seq[Integer](null)) ::
        Row(Seq[Integer](1)) ::
        Row(Seq[Integer](2)) ::
        Row(null) ::
        Row(Seq[Integer](2, 3)) ::
        Row(Seq[Integer](2, 3, 4)) ::
        Row(Seq[Integer](3)) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT value1, key
          |FROM agg3
          |GROUP BY value1, key
        """.stripMargin),
      Row(10, Seq[Integer](1, 1)) ::
        Row(-60, Seq[Integer](null)) ::
        Row(30, Seq[Integer](1, 1)) ::
        Row(30, Seq[Integer](1)) ::
        Row(1, Seq[Integer](2)) ::
        Row(-10, null) ::
        Row(-1, Seq[Integer](2, 3)) ::
        Row(1, Seq[Integer](2, 3)) ::
        Row(null, Seq[Integer](2, 3, 4)) ::
        Row(100, Seq[Integer](null)) ::
        Row(null, Seq[Integer](3)) ::
        Row(null, null) :: Nil)
  }

  test("case in-sensitive resolution") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value), kEY - 100
          |FROM agg1
          |GROUP BY Key - 100
        """.stripMargin),
      Row(20.0, -99) :: Row(-0.5, -98) :: Row(null, -97) :: Row(10.0, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT sum(distinct value1), kEY - 100, count(distinct value1)
          |FROM agg2
          |GROUP BY Key - 100
        """.stripMargin),
      Row(40, -99, 2) :: Row(0, -98, 2) :: Row(null, -97, 0) :: Row(30, null, 3) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT valUe * key - 100
          |FROM agg1
          |GROUP BY vAlue * keY - 100
        """.stripMargin),
      Row(-90) ::
        Row(-80) ::
        Row(-70) ::
        Row(-100) ::
        Row(-102) ::
        Row(null) :: Nil)
  }

  test("test average no key in output") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(-0.5) :: Row(20.0) :: Row(null) :: Row(10.0) :: Nil)
  }

  test("test average") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT key, avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(1, 20.0) :: Row(2, -0.5) :: Row(3, null) :: Row(null, 10.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT key, mean(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(1, 20.0) :: Row(2, -0.5) :: Row(3, null) :: Row(null, 10.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value), key
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(20.0, 1) :: Row(-0.5, 2) :: Row(null, 3) :: Row(10.0, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value) + 1.5, key + 10
          |FROM agg1
          |GROUP BY key + 10
        """.stripMargin),
      Row(21.5, 11) :: Row(1.0, 12) :: Row(null, 13) :: Row(11.5, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value) FROM agg1
        """.stripMargin),
      Row(11.125) :: Nil)
  }

  test("first_value and last_value") {
    // We force to use a single partition for the sort and aggregate to make result
    // deterministic.
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      checkAnswer(
        sqlContext.sql(
          """
            |SELECT
            |  first_valUE(key),
            |  lasT_value(key),
            |  firSt(key),
            |  lASt(key),
            |  first_valUE(key, true),
            |  lasT_value(key, true),
            |  firSt(key, true),
            |  lASt(key, true)
            |FROM (SELECT key FROM agg1 ORDER BY key) tmp
          """.stripMargin),
        Row(null, 3, null, 3, 1, 3, 1, 3) :: Nil)

      checkAnswer(
        sqlContext.sql(
          """
            |SELECT
            |  first_valUE(key),
            |  lasT_value(key),
            |  firSt(key),
            |  lASt(key),
            |  first_valUE(key, true),
            |  lasT_value(key, true),
            |  firSt(key, true),
            |  lASt(key, true)
            |FROM (SELECT key FROM agg1 ORDER BY key DESC) tmp
          """.stripMargin),
        Row(3, null, 3, null, 3, 1, 3, 1) :: Nil)
    }
  }

  test("udaf") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  key,
          |  mydoublesum(value + 1.5 * key),
          |  mydoubleavg(value),
          |  avg(value - key),
          |  mydoublesum(value - 1.5 * key),
          |  avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(1, 64.5, 120.0, 19.0, 55.5, 20.0) ::
        Row(2, 5.0, 99.5, -2.5, -7.0, -0.5) ::
        Row(3, null, null, null, null, null) ::
        Row(null, null, 110.0, null, null, 10.0) :: Nil)
  }

  test("interpreted aggregate function") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT mydoublesum(value), key
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(60.0, 1) :: Row(-1.0, 2) :: Row(null, 3) :: Row(30.0, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT mydoublesum(value) FROM agg1
        """.stripMargin),
      Row(89.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT mydoublesum(null)
        """.stripMargin),
      Row(null) :: Nil)
  }

  test("interpreted and expression-based aggregation functions") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT mydoublesum(value), key, avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(60.0, 1, 20.0) ::
        Row(-1.0, 2, -0.5) ::
        Row(null, 3, null) ::
        Row(30.0, null, 10.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  mydoublesum(value + 1.5 * key),
          |  avg(value - key),
          |  key,
          |  mydoublesum(value - 1.5 * key),
          |  avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(64.5, 19.0, 1, 55.5, 20.0) ::
        Row(5.0, -2.5, 2, -7.0, -0.5) ::
        Row(null, null, 3, null, null) ::
        Row(null, null, null, null, 10.0) :: Nil)
  }

  test("single distinct column set") {
    Seq(true, false).foreach { specializeSingleDistinctAgg =>
      val conf =
        (SQLConf.SPECIALIZE_SINGLE_DISTINCT_AGG_PLANNING.key,
          specializeSingleDistinctAgg.toString)
      withSQLConf(conf) {
        // DISTINCT is not meaningful with Max and Min, so we just ignore the DISTINCT keyword.
        checkAnswer(
          sqlContext.sql(
            """
              |SELECT
              |  min(distinct value1),
              |  sum(distinct value1),
              |  avg(value1),
              |  avg(value2),
              |  max(distinct value1)
              |FROM agg2
            """.stripMargin),
          Row(-60, 70.0, 101.0/9.0, 5.6, 100))

        checkAnswer(
          sqlContext.sql(
            """
              |SELECT
              |  mydoubleavg(distinct value1),
              |  avg(value1),
              |  avg(value2),
              |  key,
              |  mydoubleavg(value1 - 1),
              |  mydoubleavg(distinct value1) * 0.1,
              |  avg(value1 + value2)
              |FROM agg2
              |GROUP BY key
            """.stripMargin),
          Row(120.0, 70.0/3.0, -10.0/3.0, 1, 67.0/3.0 + 100.0, 12.0, 20.0) ::
            Row(100.0, 1.0/3.0, 1.0, 2, -2.0/3.0 + 100.0, 10.0, 2.0) ::
            Row(null, null, 3.0, 3, null, null, null) ::
            Row(110.0, 10.0, 20.0, null, 109.0, 11.0, 30.0) :: Nil)

        checkAnswer(
          sqlContext.sql(
            """
              |SELECT
              |  key,
              |  mydoubleavg(distinct value1),
              |  mydoublesum(value2),
              |  mydoublesum(distinct value1),
              |  mydoubleavg(distinct value1),
              |  mydoubleavg(value1)
              |FROM agg2
              |GROUP BY key
            """.stripMargin),
          Row(1, 120.0, -10.0, 40.0, 120.0, 70.0/3.0 + 100.0) ::
            Row(2, 100.0, 3.0, 0.0, 100.0, 1.0/3.0 + 100.0) ::
            Row(3, null, 3.0, null, null, null) ::
            Row(null, 110.0, 60.0, 30.0, 110.0, 110.0) :: Nil)

        checkAnswer(
          sqlContext.sql(
            """
              |SELECT
              |  count(value1),
              |  count(*),
              |  count(1),
              |  count(DISTINCT value1),
              |  key
              |FROM agg2
              |GROUP BY key
            """.stripMargin),
          Row(3, 3, 3, 2, 1) ::
            Row(3, 4, 4, 2, 2) ::
            Row(0, 2, 2, 0, 3) ::
            Row(3, 4, 4, 3, null) :: Nil)
      }
    }
  }

  test("single distinct multiple columns set") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  key,
          |  count(distinct value1, value2)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(null, 3) ::
        Row(1, 3) ::
        Row(2, 1) ::
        Row(3, 0) :: Nil)
  }

  test("multiple distinct multiple columns sets") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  key,
          |  count(distinct value1),
          |  sum(distinct value1),
          |  count(distinct value2),
          |  sum(distinct value2),
          |  count(distinct value1, value2),
          |  longProductSum(distinct value1, value2),
          |  count(value1),
          |  sum(value1),
          |  count(value2),
          |  sum(value2),
          |  longProductSum(value1, value2),
          |  count(*),
          |  count(1)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(null, 3, 30, 3, 60, 3, -4700, 3, 30, 3, 60, -4700, 4, 4) ::
        Row(1, 2, 40, 3, -10, 3, -100, 3, 70, 3, -10, -100, 3, 3) ::
        Row(2, 2, 0, 1, 1, 1, 1, 3, 1, 3, 3, 2, 4, 4) ::
        Row(3, 0, null, 1, 3, 0, 0, 0, null, 1, 3, 0, 2, 2) :: Nil)
  }

  test("test count") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  count(value2),
          |  value1,
          |  count(*),
          |  count(1),
          |  key
          |FROM agg2
          |GROUP BY key, value1
        """.stripMargin),
      Row(1, 10, 1, 1, 1) ::
        Row(1, -60, 1, 1, null) ::
        Row(2, 30, 2, 2, 1) ::
        Row(2, 1, 2, 2, 2) ::
        Row(1, -10, 1, 1, null) ::
        Row(0, -1, 1, 1, 2) ::
        Row(1, null, 1, 1, 2) ::
        Row(1, 100, 1, 1, null) ::
        Row(1, null, 2, 2, 3) ::
        Row(0, null, 1, 1, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  count(value2),
          |  value1,
          |  count(*),
          |  count(1),
          |  key,
          |  count(DISTINCT abs(value2))
          |FROM agg2
          |GROUP BY key, value1
        """.stripMargin),
      Row(1, 10, 1, 1, 1, 1) ::
        Row(1, -60, 1, 1, null, 1) ::
        Row(2, 30, 2, 2, 1, 1) ::
        Row(2, 1, 2, 2, 2, 1) ::
        Row(1, -10, 1, 1, null, 1) ::
        Row(0, -1, 1, 1, 2, 0) ::
        Row(1, null, 1, 1, 2, 1) ::
        Row(1, 100, 1, 1, null, 1) ::
        Row(1, null, 2, 2, 3, 1) ::
        Row(0, null, 1, 1, null, 0) :: Nil)
  }

  test("pearson correlation") {
    val df = Seq.tabulate(10)(i => (1.0 * i, 2.0 * i, i * -1.0)).toDF("a", "b", "c")
    val corr1 = df.repartition(2).groupBy().agg(corr("a", "b")).collect()(0).getDouble(0)
    assert(math.abs(corr1 - 1.0) < 1e-12)
    val corr2 = df.groupBy().agg(corr("a", "c")).collect()(0).getDouble(0)
    assert(math.abs(corr2 + 1.0) < 1e-12)
    // non-trivial example. To reproduce in python, use:
    // >>> from scipy.stats import pearsonr
    // >>> import numpy as np
    // >>> a = np.array(range(20))
    // >>> b = np.array([x * x - 2 * x + 3.5 for x in range(20)])
    // >>> pearsonr(a, b)
    // (0.95723391394758572, 3.8902121417802199e-11)
    // In R, use:
    // > a <- 0:19
    // > b <- mapply(function(x) x * x - 2 * x + 3.5, a)
    // > cor(a, b)
    // [1] 0.957233913947585835
    val df2 = Seq.tabulate(20)(x => (1.0 * x, x * x - 2 * x + 3.5)).toDF("a", "b")
    val corr3 = df2.groupBy().agg(corr("a", "b")).collect()(0).getDouble(0)
    assert(math.abs(corr3 - 0.95723391394758572) < 1e-12)

    val df3 = Seq.tabulate(0)(i => (1.0 * i, 2.0 * i)).toDF("a", "b")
    val corr4 = df3.groupBy().agg(corr("a", "b")).collect()(0)
    assert(corr4 == Row(null))

    val df4 = Seq.tabulate(10)(i => (1 * i, 2 * i, i * -1)).toDF("a", "b", "c")
    val corr5 = df4.repartition(2).groupBy().agg(corr("a", "b")).collect()(0).getDouble(0)
    assert(math.abs(corr5 - 1.0) < 1e-12)
    val corr6 = df4.groupBy().agg(corr("a", "c")).collect()(0).getDouble(0)
    assert(math.abs(corr6 + 1.0) < 1e-12)

    // Test for udaf_corr in HiveCompatibilitySuite
    // udaf_corr has been blacklisted due to numerical errors
    // We test it here:
    // SELECT corr(b, c) FROM covar_tab WHERE a < 1; => NULL
    // SELECT corr(b, c) FROM covar_tab WHERE a < 3; => NULL
    // SELECT corr(b, c) FROM covar_tab WHERE a = 3; => NULL
    // SELECT a, corr(b, c) FROM covar_tab GROUP BY a ORDER BY a; =>
    // 1       NULL
    // 2       NULL
    // 3       NULL
    // 4       NULL
    // 5       NULL
    // 6       NULL
    // SELECT corr(b, c) FROM covar_tab; => 0.6633880657639323

    val covar_tab = Seq[(Integer, Integer, Integer)](
      (1, null, 15),
      (2, 3, null),
      (3, 7, 12),
      (4, 4, 14),
      (5, 8, 17),
      (6, 2, 11)).toDF("a", "b", "c")

    covar_tab.registerTempTable("covar_tab")

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT corr(b, c) FROM covar_tab WHERE a < 1
        """.stripMargin),
      Row(null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT corr(b, c) FROM covar_tab WHERE a < 3
        """.stripMargin),
      Row(null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT corr(b, c) FROM covar_tab WHERE a = 3
        """.stripMargin),
      Row(null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT a, corr(b, c) FROM covar_tab GROUP BY a ORDER BY a
        """.stripMargin),
      Row(1, null) ::
      Row(2, null) ::
      Row(3, null) ::
      Row(4, null) ::
      Row(5, null) ::
      Row(6, null) :: Nil)

    val corr7 = sqlContext.sql("SELECT corr(b, c) FROM covar_tab").collect()(0).getDouble(0)
    assert(math.abs(corr7 - 0.6633880657639323) < 1e-12)
  }

  test("no aggregation function (SPARK-11486)") {
    val df = sqlContext.range(20).selectExpr("id", "repeat(id, 1) as s")
      .groupBy("s").count()
      .groupBy().count()
    checkAnswer(df, Row(20) :: Nil)
  }

  test("udaf with all data types") {
    val struct =
      StructType(
        StructField("f1", FloatType, true) ::
          StructField("f2", ArrayType(BooleanType), true) :: Nil)
    val dataTypes = Seq(StringType, BinaryType, NullType, BooleanType,
      ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
      DateType, TimestampType,
      ArrayType(IntegerType), MapType(StringType, LongType), struct,
      new MyDenseVectorUDT())
    // Right now, we will use SortBasedAggregate to handle UDAFs.
    // UnsafeRow.mutableFieldTypes.asScala.toSeq will trigger SortBasedAggregate to use
    // UnsafeRow as the aggregation buffer. While, dataTypes will trigger
    // SortBasedAggregate to use a safe row as the aggregation buffer.
    Seq(dataTypes, UnsafeRow.mutableFieldTypes.asScala.toSeq).foreach { dataTypes =>
      val fields = dataTypes.zipWithIndex.map { case (dataType, index) =>
        StructField(s"col$index", dataType, nullable = true)
      }
      // The schema used for data generator.
      val schemaForGenerator = StructType(fields)
      // The schema used for the DataFrame df.
      val schema = StructType(StructField("id", IntegerType) +: fields)

      logInfo(s"Testing schema: ${schema.treeString}")

      val udaf = new ScalaAggregateFunction(schema)
      // Generate data at the driver side. We need to materialize the data first and then
      // create RDD.
      val maybeDataGenerator =
        RandomDataGenerator.forType(
          dataType = schemaForGenerator,
          nullable = true,
          seed = Some(System.nanoTime()))
      val dataGenerator =
        maybeDataGenerator
          .getOrElse(fail(s"Failed to create data generator for schema $schemaForGenerator"))
      val data = (1 to 50).map { i =>
        dataGenerator.apply() match {
          case row: Row => Row.fromSeq(i +: row.toSeq)
          case null => Row.fromSeq(i +: Seq.fill(schemaForGenerator.length)(null))
          case other =>
            fail(s"Row or null is expected to be generated, " +
              s"but a ${other.getClass.getCanonicalName} is generated.")
        }
      }

      // Create a DF for the schema with random data.
      val rdd = sqlContext.sparkContext.parallelize(data, 1)
      val df = sqlContext.createDataFrame(rdd, schema)

      val allColumns = df.schema.fields.map(f => col(f.name))
      val expectedAnswer =
        data
          .find(r => r.getInt(0) == 50)
          .getOrElse(fail("A row with id 50 should be the expected answer."))
      checkAnswer(
        df.groupBy().agg(udaf(allColumns: _*)),
        // udaf returns a Row as the output value.
        Row(expectedAnswer)
      )
    }
  }

  test("udaf without specifying inputSchema") {
    withTempTable("noInputSchemaUDAF") {
      sqlContext.udf.register("noInputSchema", new ScalaAggregateFunctionWithoutInputSchema)

      val data =
        Row(1, Seq(Row(1), Row(2), Row(3))) ::
          Row(1, Seq(Row(4), Row(5), Row(6))) ::
          Row(2, Seq(Row(-10))) :: Nil
      val schema =
        StructType(
          StructField("key", IntegerType) ::
            StructField("myArray",
              ArrayType(StructType(StructField("v", IntegerType) :: Nil))) :: Nil)
      sqlContext.createDataFrame(
        sparkContext.parallelize(data, 2),
        schema)
        .registerTempTable("noInputSchemaUDAF")

      checkAnswer(
        sqlContext.sql(
          """
            |SELECT key, noInputSchema(myArray)
            |FROM noInputSchemaUDAF
            |GROUP BY key
          """.stripMargin),
        Row(1, 21) :: Row(2, -10) :: Nil)

      checkAnswer(
        sqlContext.sql(
          """
            |SELECT noInputSchema(myArray)
            |FROM noInputSchemaUDAF
          """.stripMargin),
        Row(11) :: Nil)
    }
  }
}


class TungstenAggregationQuerySuite extends AggregationQuerySuite


class TungstenAggregationQueryWithControlledFallbackSuite extends AggregationQuerySuite {

  override protected def checkAnswer(actual: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    (0 to 2).foreach { fallbackStartsAt =>
      withSQLConf("spark.sql.TungstenAggregate.testFallbackStartsAt" -> fallbackStartsAt.toString) {
        // Create a new df to make sure its physical operator picks up
        // spark.sql.TungstenAggregate.testFallbackStartsAt.
        // todo: remove it?
        val newActual = DataFrame(sqlContext, actual.logicalPlan)

        QueryTest.checkAnswer(newActual, expectedAnswer) match {
          case Some(errorMessage) =>
            val newErrorMessage =
              s"""
                |The following aggregation query failed when using TungstenAggregate with
                |controlled fallback (it falls back to sort-based aggregation once it has processed
                |$fallbackStartsAt input rows). The query is
                |${actual.queryExecution}
                |
                |$errorMessage
              """.stripMargin

            fail(newErrorMessage)
          case None =>
        }
      }
    }
  }

  // Override it to make sure we call the actually overridden checkAnswer.
  override protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  // Override it to make sure we call the actually overridden checkAnswer.
  override protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }
}
