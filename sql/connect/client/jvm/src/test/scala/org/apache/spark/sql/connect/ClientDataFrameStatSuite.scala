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

package org.apache.spark.sql.connect

import java.util.Random

import org.scalatest.matchers.must.Matchers._

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

class ClientDataFrameStatSuite extends ConnectFunSuite with RemoteSparkSession {
  private def toLetter(i: Int): String = (i + 97).toChar.toString

  test("approxQuantile") {
    val session = spark
    import session.implicits._

    val n = 1000
    val df = Seq.tabulate(n + 1)(i => (i, 2.0 * i)).toDF("singles", "doubles")

    val q1 = 0.5
    val q2 = 0.8
    val epsilons = List(0.1, 0.05, 0.001)

    for (epsilon <- epsilons) {
      val Array(single1) = df.stat.approxQuantile("singles", Array(q1), epsilon)
      val Array(double2) = df.stat.approxQuantile("doubles", Array(q2), epsilon)
      // Also make sure there is no regression by computing multiple quantiles at once.
      val Array(d1, d2) = df.stat.approxQuantile("doubles", Array(q1, q2), epsilon)
      val Array(s1, s2) = df.stat.approxQuantile("singles", Array(q1, q2), epsilon)

      val errorSingle = 1000 * epsilon
      val errorDouble = 2.0 * errorSingle

      assert(math.abs(single1 - q1 * n) <= errorSingle)
      assert(math.abs(double2 - 2 * q2 * n) <= errorDouble)
      assert(math.abs(s1 - q1 * n) <= errorSingle)
      assert(math.abs(s2 - q2 * n) <= errorSingle)
      assert(math.abs(d1 - 2 * q1 * n) <= errorDouble)
      assert(math.abs(d2 - 2 * q2 * n) <= errorDouble)

      // Multiple columns
      val Array(Array(ms1, ms2), Array(md1, md2)) =
        df.stat.approxQuantile(Array("singles", "doubles"), Array(q1, q2), epsilon)

      assert(math.abs(ms1 - q1 * n) <= errorSingle)
      assert(math.abs(ms2 - q2 * n) <= errorSingle)
      assert(math.abs(md1 - 2 * q1 * n) <= errorDouble)
      assert(math.abs(md2 - 2 * q2 * n) <= errorDouble)
    }

    // quantile should be in the range [0.0, 1.0]
    val e = intercept[IllegalArgumentException] {
      df.stat.approxQuantile(Array("singles", "doubles"), Array(q1, q2, -0.1), epsilons.head)
    }
    assert(e.getMessage.contains("percentile should be in the range [0.0, 1.0]"))

    // relativeError should be non-negative
    val e2 = intercept[IllegalArgumentException] {
      df.stat.approxQuantile(Array("singles", "doubles"), Array(q1, q2), -1.0)
    }
    assert(e2.getMessage.contains("Relative Error must be non-negative"))
  }

  test("covariance") {
    val session = spark
    import session.implicits._

    val df =
      Seq.tabulate(10)(i => (i, 2.0 * i, toLetter(i))).toDF("singles", "doubles", "letters")

    val results = df.stat.cov("singles", "doubles")
    assert(math.abs(results - 55.0 / 3) < 1e-12)
    intercept[SparkIllegalArgumentException] {
      df.stat.cov("singles", "letters") // doesn't accept non-numerical dataTypes
    }
    val decimalData = Seq.tabulate(6)(i => (BigDecimal(i % 3), BigDecimal(i % 2))).toDF("a", "b")
    val decimalRes = decimalData.stat.cov("a", "b")
    assert(math.abs(decimalRes) < 1e-12)
  }

  test("correlation") {
    val session = spark
    import session.implicits._

    val df = Seq.tabulate(10)(i => (i, 2 * i, i * -1.0)).toDF("a", "b", "c")
    val corr1 = df.stat.corr("a", "b", "pearson")
    assert(math.abs(corr1 - 1.0) < 1e-12)
    val corr2 = df.stat.corr("a", "c", "pearson")
    assert(math.abs(corr2 + 1.0) < 1e-12)
    val df2 = Seq.tabulate(20)(x => (x, x * x - 2 * x + 3.5)).toDF("a", "b")
    val corr3 = df2.stat.corr("a", "b", "pearson")
    assert(math.abs(corr3 - 0.95723391394758572) < 1e-12)
  }

  test("crosstab") {
    val session = spark
    import session.implicits._

    val rng = new Random()
    val data = Seq.tabulate(25)(_ => (rng.nextInt(5), rng.nextInt(10)))
    val df = data.toDF("a", "b")
    val crosstab = df.stat.crosstab("a", "b")
    val columnNames = crosstab.schema.fieldNames
    assert(columnNames(0) === "a_b")
    // reduce by key
    val expected = data.map(t => (t, 1)).groupBy(_._1).transform((_, v) => v.length)
    val rows = crosstab.collect()
    rows.foreach { row =>
      val i = row.getString(0).toInt
      for (col <- 1 until columnNames.length) {
        val j = columnNames(col).toInt
        assert(row.getLong(col) === expected.getOrElse((i, j), 0).toLong)
      }
    }
  }

  test("freqItems") {
    val session = spark
    import session.implicits._

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

  test("sampleBy") {
    val session = spark
    import session.implicits._
    val df = Seq("Bob", "Alice", "Nico", "Bob", "Alice").toDF("name")
    val fractions = Map("Alice" -> 0.3, "Nico" -> 1.0)
    val sampled = df.stat.sampleBy("name", fractions, 36L)
    val rows = sampled.groupBy("name").count().orderBy("name").collect()
    assert(rows.length == 1)
    val row0 = rows(0)
    assert(row0.getString(0) == "Nico")
    assert(row0.getLong(1) == 1L)
  }

  test("countMinSketch") {
    val df = spark.range(1000)

    val sketch1 = df.stat.countMinSketch("id", depth = 10, width = 20, seed = 42)
    assert(sketch1.totalCount() === 1000)
    assert(sketch1.depth() === 10)
    assert(sketch1.width() === 20)

    val sketch = df.stat.countMinSketch("id", eps = 0.001, confidence = 0.99, seed = 42)
    assert(sketch.totalCount() === 1000)
    assert(sketch.relativeError() === 0.001)
    assert(sketch.confidence() === 0.99 +- 5e-3)
  }

  test("Bloom filter -- Long Column") {
    val session = spark
    import session.implicits._
    val data = Seq(-143, -32, -5, 1, 17, 39, 43, 101, 127, 997).map(_.toLong)
    val df = data.toDF("id")
    val negativeValues = Seq(-11, 1021, 32767).map(_.toLong)
    checkBloomFilter(data, negativeValues, df)
  }

  test("Bloom filter -- Int Column") {
    val session = spark
    import session.implicits._
    val data = Seq(-143, -32, -5, 1, 17, 39, 43, 101, 127, 997)
    val df = data.toDF("id")
    val negativeValues = Seq(-11, 1021, 32767)
    checkBloomFilter(data, negativeValues, df)
  }

  test("Bloom filter -- Short Column") {
    val session = spark
    import session.implicits._
    val data = Seq(-143, -32, -5, 1, 17, 39, 43, 101, 127, 997).map(_.toShort)
    val df = data.toDF("id")
    val negativeValues = Seq(-11, 1021, 32767).map(_.toShort)
    checkBloomFilter(data, negativeValues, df)
  }

  test("Bloom filter -- Byte Column") {
    val session = spark
    import session.implicits._
    val data = Seq(-32, -5, 1, 17, 39, 43, 101, 127).map(_.toByte)
    val df = data.toDF("id")
    val negativeValues = Seq(-101, 55, 113).map(_.toByte)
    checkBloomFilter(data, negativeValues, df)
  }

  test("Bloom filter -- String Column") {
    val session = spark
    import session.implicits._
    val data = Seq(-143, -32, -5, 1, 17, 39, 43, 101, 127, 997).map(_.toString)
    val df = data.toDF("id")
    val negativeValues = Seq(-11, 1021, 32767).map(_.toString)
    checkBloomFilter(data, negativeValues, df)
  }

  private def checkBloomFilter(
      data: Seq[Any],
      notContainValues: Seq[Any],
      df: DataFrame): Unit = {
    val filter1 = df.stat.bloomFilter("id", 1000, 0.03)
    assert(filter1.expectedFpp() - 0.03 < 1e-3)
    assert(data.forall(filter1.mightContain))
    assert(notContainValues.forall(n => !filter1.mightContain(n)))
    val filter2 = df.stat.bloomFilter("id", 1000, 64 * 5)
    assert(filter2.bitSize() == 64 * 5)
    assert(data.forall(filter2.mightContain))
    assert(notContainValues.forall(n => !filter2.mightContain(n)))
  }

  test("Bloom filter -- Wrong dataType Column") {
    val session = spark
    import session.implicits._
    val data = Range(0, 1000).map(_.toDouble)
    val message = intercept[AnalysisException] {
      data.toDF("id").stat.bloomFilter("id", 1000, 0.03)
    }.getMessage
    assert(message.contains("DATATYPE_MISMATCH.BLOOM_FILTER_WRONG_TYPE"))
  }

  test("Bloom filter test invalid inputs") {
    val df = spark.range(1000).toDF("id")
    val error1 = intercept[AnalysisException] {
      df.stat.bloomFilter("id", -1000, 100)
    }
    assert(error1.getCondition === "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE")

    val error2 = intercept[AnalysisException] {
      df.stat.bloomFilter("id", 1000, -100)
    }
    assert(error2.getCondition === "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE")

    val error3 = intercept[AnalysisException] {
      df.stat.bloomFilter("id", 1000, -1.0)
    }
    assert(error3.getCondition === "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE")
  }

  test("SPARK-49961: transform type should be consistent") {
    val session = spark
    import session.implicits._
    val ds = Seq(1, 2).toDS()
    val f: Dataset[Int] => Dataset[Int] = d => d.selectExpr("(value + 1) value").as[Int]
    val transformed = ds.transform(f)
    assert(transformed.collect().sorted === Array(2, 3))
  }
}
