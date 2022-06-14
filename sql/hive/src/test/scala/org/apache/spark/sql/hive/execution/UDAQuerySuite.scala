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

import java.lang.{Double => jlDouble, Long => jlLong}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowHiveTest

class MyDoubleAvgAggBase extends Aggregator[jlDouble, (Double, Long), jlDouble] {
  def zero: (Double, Long) = (0.0, 0L)
  def reduce(b: (Double, Long), a: jlDouble): (Double, Long) = {
    if (a != null) (b._1 + a, b._2 + 1L) else b
  }
  def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) =
    (b1._1 + b2._1, b1._2 + b2._2)
  def finish(r: (Double, Long)): jlDouble =
    if (r._2 > 0L) 100.0 + (r._1 / r._2.toDouble) else null
  def bufferEncoder: Encoder[(Double, Long)] =
    Encoders.tuple(Encoders.scalaDouble, Encoders.scalaLong)
  def outputEncoder: Encoder[jlDouble] = Encoders.DOUBLE
}

object MyDoubleAvgAgg extends MyDoubleAvgAggBase
object MyDoubleSumAgg extends MyDoubleAvgAggBase {
  override def finish(r: (Double, Long)): jlDouble = if (r._2 > 0L) r._1 else null
}

object LongProductSumAgg extends Aggregator[(jlLong, jlLong), Long, jlLong] {
  def zero: Long = 0L
  def reduce(b: Long, a: (jlLong, jlLong)): Long = {
    if ((a._1 != null) && (a._2 != null)) b + (a._1 * a._2) else b
  }
  def merge(b1: Long, b2: Long): Long = b1 + b2
  def finish(r: Long): jlLong = r
  def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  def outputEncoder: Encoder[jlLong] = Encoders.LONG
}

@SQLUserDefinedType(udt = classOf[CountSerDeUDT])
case class CountSerDeSQL(nSer: Int, nDeSer: Int, sum: Int)

class CountSerDeUDT extends UserDefinedType[CountSerDeSQL] {
  def userClass: Class[CountSerDeSQL] = classOf[CountSerDeSQL]

  override def typeName: String = "count-ser-de"

  private[spark] override def asNullable: CountSerDeUDT = this

  def sqlType: DataType = StructType(
    StructField("nSer", IntegerType, false) ::
    StructField("nDeSer", IntegerType, false) ::
    StructField("sum", IntegerType, false) ::
    Nil)

  def serialize(sql: CountSerDeSQL): Any = {
    val row = new GenericInternalRow(3)
    row.setInt(0, 1 + sql.nSer)
    row.setInt(1, sql.nDeSer)
    row.setInt(2, sql.sum)
    row
  }

  def deserialize(any: Any): CountSerDeSQL = any match {
    case row: InternalRow if (row.numFields == 3) =>
      CountSerDeSQL(row.getInt(0), 1 + row.getInt(1), row.getInt(2))
    case u => throw new Exception(s"failed to deserialize: $u")
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case _: CountSerDeUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[CountSerDeUDT].getName.hashCode()
}

case object CountSerDeUDT extends CountSerDeUDT

object CountSerDeAgg extends Aggregator[Int, CountSerDeSQL, CountSerDeSQL] {
  def zero: CountSerDeSQL = CountSerDeSQL(0, 0, 0)
  def reduce(b: CountSerDeSQL, a: Int): CountSerDeSQL = b.copy(sum = b.sum + a)
  def merge(b1: CountSerDeSQL, b2: CountSerDeSQL): CountSerDeSQL =
    CountSerDeSQL(b1.nSer + b2.nSer, b1.nDeSer + b2.nDeSer, b1.sum + b2.sum)
  def finish(r: CountSerDeSQL): CountSerDeSQL = r
  def bufferEncoder: Encoder[CountSerDeSQL] = ExpressionEncoder[CountSerDeSQL]()
  def outputEncoder: Encoder[CountSerDeSQL] = ExpressionEncoder[CountSerDeSQL]()
}

object ArrayDataAgg extends Aggregator[Array[Double], Array[Double], Array[Double]] {
  def zero: Array[Double] = Array(0.0, 0.0, 0.0)
  def reduce(s: Array[Double], array: Array[Double]): Array[Double] = {
    require(s.length == array.length)
    for ( j <- s.indices) {
      s(j) += array(j)
    }
    s
  }
  def merge(s1: Array[Double], s2: Array[Double]): Array[Double] = {
    require(s1.length == s2.length)
    for ( j <- s1.indices) {
      s1(j) += s2(j)
    }
    s1
  }
  def finish(s: Array[Double]): Array[Double] = s
  def bufferEncoder: Encoder[Array[Double]] = ExpressionEncoder[Array[Double]]
  def outputEncoder: Encoder[Array[Double]] = ExpressionEncoder[Array[Double]]
}

abstract class UDAQuerySuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
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

    val data3 = Seq[(Seq[Double], Int)](
      (Seq(1.0, 2.0, 3.0), 0),
      (Seq(4.0, 5.0, 6.0), 0),
      (Seq(7.0, 8.0, 9.0), 0)
    ).toDF("data", "dummy")
    data3.write.saveAsTable("agg3")

    val data4 = Seq[Boolean](true, false, true).toDF("boolvalues")
    data4.write.saveAsTable("agg4")

    val emptyDF = spark.createDataFrame(
      sparkContext.emptyRDD[Row],
      StructType(StructField("key", StringType) :: StructField("value", IntegerType) :: Nil))
    emptyDF.createOrReplaceTempView("emptyTable")

    // Register UDAs
    spark.udf.register("mydoublesum", udaf(MyDoubleSumAgg))
    spark.udf.register("mydoubleavg", udaf(MyDoubleAvgAgg))
    spark.udf.register("longProductSum", udaf(LongProductSumAgg))
    spark.udf.register("arraysum", udaf(ArrayDataAgg))
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS agg1")
      spark.sql("DROP TABLE IF EXISTS agg2")
      spark.sql("DROP TABLE IF EXISTS agg3")
      spark.sql("DROP TABLE IF EXISTS agg4")
      spark.catalog.dropTempView("emptyTable")
    } finally {
      super.afterAll()
    }
  }

  test("aggregators") {
    checkAnswer(
      spark.sql(
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

  test("non-deterministic children expressions of aggregator") {
    val e = intercept[AnalysisException] {
      spark.sql(
        """
          |SELECT mydoublesum(value + 1.5 * key + rand())
          |FROM agg1
          |GROUP BY key
        """.stripMargin)
    }.getMessage
    assert(Seq("nondeterministic expression",
      "should not appear in the arguments of an aggregate function").forall(e.contains))
  }

  test("interpreted aggregate function") {
    checkAnswer(
      spark.sql(
        """
          |SELECT mydoublesum(value), key
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(60.0, 1) :: Row(-1.0, 2) :: Row(null, 3) :: Row(30.0, null) :: Nil)

    checkAnswer(
      spark.sql(
        """
          |SELECT mydoublesum(value) FROM agg1
        """.stripMargin),
      Row(89.0) :: Nil)

    checkAnswer(
      spark.sql(
        """
          |SELECT mydoublesum(null)
        """.stripMargin),
      Row(null) :: Nil)
  }

  test("interpreted and expression-based aggregation functions") {
    checkAnswer(
      spark.sql(
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
      spark.sql(
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
    checkAnswer(
      spark.sql(
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
      spark.sql(
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
  }

  test("multiple distinct multiple columns sets") {
    checkAnswer(
      spark.sql(
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

  test("SPARK-32159: array encoders should be resolved in analyzer") {
    checkAnswer(
      spark.sql("SELECT arraysum(data) FROM agg3"),
      Row(Seq(12.0, 15.0, 18.0)) :: Nil)
  }

  test("verify aggregator ser/de behavior") {
    val data = sparkContext.parallelize((1 to 100).toSeq, 3).toDF("value1")
    val agg = udaf(CountSerDeAgg)
    checkAnswer(
      data.agg(agg($"value1")),
      Row(CountSerDeSQL(4, 4, 5050)) :: Nil)
  }

  test("verify type casting failure") {
    assertThrows[org.apache.spark.sql.AnalysisException] {
      spark.sql(
        """
          |SELECT mydoublesum(boolvalues) FROM agg4
        """.stripMargin)
    }
  }
}

@SlowHiveTest
class HashUDAQuerySuite extends UDAQuerySuite

@SlowHiveTest
class HashUDAQueryWithControlledFallbackSuite extends UDAQuerySuite {

  override protected def checkAnswer(actual: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    super.checkAnswer(actual, expectedAnswer)
    Seq("true", "false").foreach { enableTwoLevelMaps =>
      withSQLConf("spark.sql.codegen.aggregate.map.twolevel.enabled" ->
        enableTwoLevelMaps) {
        (1 to 3).foreach { fallbackStartsAt =>
          withSQLConf("spark.sql.TungstenAggregate.testFallbackStartsAt" ->
            s"${(fallbackStartsAt - 1).toString}, ${fallbackStartsAt.toString}") {
            QueryTest.getErrorMessageInCheckAnswer(actual, expectedAnswer) match {
              case Some(errorMessage) =>
                val newErrorMessage =
                  s"""
                     |The following aggregation query failed when using HashAggregate with
                     |controlled fallback (it falls back to bytes to bytes map once it has processed
                     |${fallbackStartsAt - 1} input rows and to sort-based aggregation once it has
                     |processed $fallbackStartsAt input rows). The query is ${actual.queryExecution}
                     |
                    |$errorMessage
                  """.stripMargin

                fail(newErrorMessage)
              case None => // Success
            }
          }
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
