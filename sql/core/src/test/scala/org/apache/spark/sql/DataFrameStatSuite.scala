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

import java.util.Random

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameStatSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private def toLetter(i: Int): String = (i + 97).toChar.toString

  test("sample with replacement") {
    val n = 100
    val data = sparkContext.parallelize(1 to n, 2).toDF("id")
    checkAnswer(
      data.sample(withReplacement = true, 0.05, seed = 13),
      Seq(5, 10, 52, 73).map(Row(_))
    )
  }

  test("sample without replacement") {
    val n = 100
    val data = sparkContext.parallelize(1 to n, 2).toDF("id")
    checkAnswer(
      data.sample(withReplacement = false, 0.05, seed = 13),
      Seq(3, 17, 27, 58, 62).map(Row(_))
    )
  }

  test("randomSplit") {
    val n = 600
    val data = sparkContext.parallelize(1 to n, 2).toDF("id")
    for (seed <- 1 to 5) {
      val splits = data.randomSplit(Array[Double](1, 2, 3), seed)
      assert(splits.length == 3, "wrong number of splits")

      assert(splits.reduce((a, b) => a.unionAll(b)).sort("id").collect().toList ==
        data.collect().toList, "incomplete or wrong split")

      val s = splits.map(_.count())
      assert(math.abs(s(0) - 100) < 50) // std =  9.13
      assert(math.abs(s(1) - 200) < 50) // std = 11.55
      assert(math.abs(s(2) - 300) < 50) // std = 12.25
    }
  }

  test("randomSplit on reordered partitions") {
    // This test ensures that randomSplit does not create overlapping splits even when the
    // underlying dataframe (such as the one below) doesn't guarantee a deterministic ordering of
    // rows in each partition.
    val data =
      sparkContext.parallelize(1 to 600, 2).mapPartitions(scala.util.Random.shuffle(_)).toDF("id")
    val splits = data.randomSplit(Array[Double](2, 3), seed = 1)

    assert(splits.length == 2, "wrong number of splits")

    // Verify that the splits span the entire dataset
    assert(splits.flatMap(_.collect()).toSet == data.collect().toSet)

    // Verify that the splits don't overalap
    assert(splits(0).intersect(splits(1)).collect().isEmpty)

    // Verify that the results are deterministic across multiple runs
    val firstRun = splits.toSeq.map(_.collect().toSeq)
    val secondRun = data.randomSplit(Array[Double](2, 3), seed = 1).toSeq.map(_.collect().toSeq)
    assert(firstRun == secondRun)
  }

  test("pearson correlation") {
    val df = Seq.tabulate(10)(i => (i, 2 * i, i * -1.0)).toDF("a", "b", "c")
    val corr1 = df.stat.corr("a", "b", "pearson")
    assert(math.abs(corr1 - 1.0) < 1e-12)
    val corr2 = df.stat.corr("a", "c", "pearson")
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
    val df2 = Seq.tabulate(20)(x => (x, x * x - 2 * x + 3.5)).toDF("a", "b")
    val corr3 = df2.stat.corr("a", "b", "pearson")
    assert(math.abs(corr3 - 0.95723391394758572) < 1e-12)
  }

  test("covariance") {
    val df = Seq.tabulate(10)(i => (i, 2.0 * i, toLetter(i))).toDF("singles", "doubles", "letters")

    val results = df.stat.cov("singles", "doubles")
    assert(math.abs(results - 55.0 / 3) < 1e-12)
    intercept[IllegalArgumentException] {
      df.stat.cov("singles", "letters") // doesn't accept non-numerical dataTypes
    }
    val decimalData = Seq.tabulate(6)(i => (BigDecimal(i % 3), BigDecimal(i % 2))).toDF("a", "b")
    val decimalRes = decimalData.stat.cov("a", "b")
    assert(math.abs(decimalRes) < 1e-12)
  }

  test("crosstab") {
    val rng = new Random()
    val data = Seq.tabulate(25)(i => (rng.nextInt(5), rng.nextInt(10)))
    val df = data.toDF("a", "b")
    val crosstab = df.stat.crosstab("a", "b")
    val columnNames = crosstab.schema.fieldNames
    assert(columnNames(0) === "a_b")
    // reduce by key
    val expected = data.map(t => (t, 1)).groupBy(_._1).mapValues(_.length)
    val rows = crosstab.collect()
    rows.foreach { row =>
      val i = row.getString(0).toInt
      for (col <- 1 until columnNames.length) {
        val j = columnNames(col).toInt
        assert(row.getLong(col) === expected.getOrElse((i, j), 0).toLong)
      }
    }
  }

  test("special crosstab elements (., '', null, ``)") {
    val data = Seq(
      ("a", Double.NaN, "ho"),
      (null, 2.0, "ho"),
      ("a.b", Double.NegativeInfinity, ""),
      ("b", Double.PositiveInfinity, "`ha`"),
      ("a", 1.0, null)
    )
    val df = data.toDF("1", "2", "3")
    val ct1 = df.stat.crosstab("1", "2")
    // column fields should be 1 + distinct elements of second column
    assert(ct1.schema.fields.length === 6)
    assert(ct1.collect().length === 4)
    val ct2 = df.stat.crosstab("1", "3")
    assert(ct2.schema.fields.length === 5)
    assert(ct2.schema.fieldNames.contains("ha"))
    assert(ct2.collect().length === 4)
    val ct3 = df.stat.crosstab("3", "2")
    assert(ct3.schema.fields.length === 6)
    assert(ct3.schema.fieldNames.contains("NaN"))
    assert(ct3.schema.fieldNames.contains("Infinity"))
    assert(ct3.schema.fieldNames.contains("-Infinity"))
    assert(ct3.collect().length === 4)
    val ct4 = df.stat.crosstab("3", "1")
    assert(ct4.schema.fields.length === 5)
    assert(ct4.schema.fieldNames.contains("null"))
    assert(ct4.schema.fieldNames.contains("a.b"))
    assert(ct4.collect().length === 4)
  }

  test("Frequent Items") {
    val rows = Seq.tabulate(1000) { i =>
      if (i % 3 == 0) (1, toLetter(1), -1.0) else (i, toLetter(i), i * -1.0)
    }
    val df = rows.toDF("numbers", "letters", "negDoubles")

    val results = df.stat.freqItems(Array("numbers", "letters"), 0.1)
    val items = results.collect().head
    assert(items.getSeq[Int](0).contains(1))
    assert(items.getSeq[String](1).contains(toLetter(1)))

    val singleColResults = df.stat.freqItems(Array("negDoubles"), 0.1)
    val items2 = singleColResults.collect().head
    assert(items2.getSeq[Double](0).contains(-1.0))
  }

  test("Frequent Items 2") {
    val rows = sparkContext.parallelize(Seq.empty[Int], 4)
    // this is a regression test, where when merging partitions, we omitted values with higher
    // counts than those that existed in the map when the map was full. This test should also fail
    // if anything like SPARK-9614 is observed once again
    val df = rows.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 3) { // must come from one of the later merges, therefore higher partition index
        Iterator("3", "3", "3", "3", "3")
      } else {
        Iterator("0", "1", "2", "3", "4")
      }
    }.toDF("a")
    val results = df.stat.freqItems(Array("a"), 0.25)
    val items = results.collect().head.getSeq[String](0)
    assert(items.contains("3"))
    assert(items.length === 1)
  }

  test("sampleBy") {
    val df = sqlContext.range(0, 100).select((col("id") % 3).as("key"))
    val sampled = df.stat.sampleBy("key", Map(0 -> 0.1, 1 -> 0.2), 0L)
    checkAnswer(
      sampled.groupBy("key").count().orderBy("key"),
      Seq(Row(0, 6), Row(1, 11)))
  }
}
