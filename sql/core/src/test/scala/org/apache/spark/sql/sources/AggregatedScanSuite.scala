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

package org.apache.spark.sql.sources

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class AggregatedScanSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleAggregatedScan(parameters("from").toInt, parameters("to").toInt)(sqlContext.sparkSession)
  }

}

case class SimpleAggregatedScan(from: Int, to: Int)(@transient val sparkSession: SparkSession)
    extends BaseRelation
    with Logging
    with AggregatedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType =
    StructType(
      StructField("a", IntegerType, nullable = true) ::
      StructField("b", LongType, nullable = false) ::
      StructField("c", StringType, nullable = false) ::
      StructField("d", DoubleType, nullable = false) ::
      StructField("e", DataTypes.createDecimalType(), nullable = false) ::
      StructField("g", IntegerType, nullable = false) ::
      StructField("f", FloatType, nullable = false) ::
      StructField("i", ByteType, nullable = false) ::
      StructField("j", ShortType, nullable = false) :: Nil)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    def unhandled(filter: Filter): Boolean = {
      filter match {
        case EqualTo(col, v) => col == "b"
        case EqualNullSafe(col, v) => col == "b"
        case LessThan(col, v: Int) => col == "b"
        case LessThanOrEqual(col, v: Int) => col == "b"
        case GreaterThan(col, v: Int) => col == "b"
        case GreaterThanOrEqual(col, v: Int) => col == "b"
        case In(col, values) => col == "b"
        case IsNull(col) => col == "b"
        case IsNotNull(col) => col == "b"
        case Not(pred) => unhandled(pred)
        case And(left, right) => unhandled(left) || unhandled(right)
        case Or(left, right) => unhandled(left) || unhandled(right)
        case _ => false
      }
    }

    filters.filter(unhandled)
  }

  override def buildScan(groupingColumns: Array[String],
      aggregateFunctions: Array[AggregateFunc],
      filters: Array[Filter]): RDD[Row] = {

    val rowBuilders = Array("a", "b", "c", "d", "e", "g", "f", "i", "j").map {
      case "a" => (i: Int) => Seq(i)
      case "b" => (i: Int) => Seq(i)
      case "c" => (i: Int) =>
        val c = (i % 2 + 'a').toChar.toString
        Seq(c * 5 + c.toUpperCase * 5)
      case "d" => (i: Int) => Seq(i)
      case "e" => (i: Int) => Seq(i)
      case "g" => (i: Int) => Seq(i % 2)
      case "f" => (i: Int) => Seq(i)
      case "i" => (i: Int) => Seq(i)
      case "j" => (i: Int) => Seq(i)
    }

    // Predicate test on integer column
    def translateFilterOnA(filter: Filter): Int => Boolean = filter match {
      case EqualTo("a", v) => (a: Int) => a == v
      case EqualNullSafe("a", v) => (a: Int) => a == v
      case LessThan("a", v: Int) => (a: Int) => a < v
      case LessThanOrEqual("a", v: Int) => (a: Int) => a <= v
      case GreaterThan("a", v: Int) => (a: Int) => a > v
      case GreaterThanOrEqual("a", v: Int) => (a: Int) => a >= v
      case In("a", values) => (a: Int) => values.map(_.asInstanceOf[Int]).toSet.contains(a)
      case IsNull("a") => (a: Int) => a == 7 // use 7 as NULL
      case IsNotNull("a") => (a: Int) => a != 7
      case Not(pred) => (a: Int) => !translateFilterOnA(pred)(a)
      case And(left, right) => (a: Int) =>
        translateFilterOnA(left)(a) && translateFilterOnA(right)(a)
      case Or(left, right) => (a: Int) =>
        translateFilterOnA(left)(a) || translateFilterOnA(right)(a)
      case _ => (a: Int) => true
    }

    // Predicate test on string column
    def translateFilterOnC(filter: Filter): String => Boolean = filter match {
      case StringStartsWith("c", v) => _.startsWith(v)
      case StringEndsWith("c", v) => _.endsWith(v)
      case StringContains("c", v) => _.contains(v)
      case EqualTo("c", v: String) => _.equals(v)
      case EqualTo("c", v: UTF8String) => sys.error("UTF8String should not appear in filters")
      case In("c", values) => (s: String) => values.map(_.asInstanceOf[String]).toSet.contains(s)
      case _ => (c: String) => true
    }

    def eval(a: Int) = {
      val c = (a % 2 + 'a').toChar.toString * 5 + (a % 2 + 'a').toChar.toString.toUpperCase * 5
      filters.forall(translateFilterOnA(_)(a)) && filters.forall(translateFilterOnC(_)(c))
    }

    def columnIndex(c: String): Int = c match {
      case "a" => 0
      case "b" => 1
      case "c" => 2
      case "d" => 3
      case "e" => 4
      case "g" => 5
      case "f" => 6
      case "i" => 7
      case "j" => 8
    }

    val filtered = sparkSession.sparkContext.parallelize(from to to).filter(eval).map { i =>
      rowBuilders.map(_(i)).reduceOption(_ ++ _).getOrElse(Seq.empty)
    }

    val grouped =
      if (groupingColumns.isEmpty) {
        filtered.map(r => ("NoSuchKey", r))
      } else {
        filtered.map(r => (groupingColumns.map(c => r(columnIndex(c))).mkString("+"),
            r ++ groupingColumns.map(c => r(columnIndex(c)))))
      }

    val l = groupingColumns.length
    val aggregated = grouped.groupByKey()
      .map { case (k, it) =>
        val ar = new ArrayBuffer[Any]
        if(l > 0) {
          for (i <- 0 until l) {
            // grouping columns
            ar += it.head(i + schema.fields.length)
          }
        }
        aggregateFunctions.foreach {
          case Sum(c, t) =>
            val i = columnIndex(c)
            var sum = 0
            it.foreach { r => sum += r(i).asInstanceOf[Int] }
            t match {
              case LongType => ar += java.lang.Long.valueOf(sum)
              case DoubleType => ar += java.lang.Double.valueOf(sum)
              case dt: DecimalType => ar += java.math.BigDecimal.valueOf(sum)
            }
          case Count(c) => c match {
            case "a" =>
              var count = 0
              it.foreach { r => if (r(0) != 7) count += 1 } // use 7 as NULL
              ar += java.lang.Long.valueOf(count)
            case _ =>
              ar += java.lang.Long.valueOf(it.size)
          }
          case CountStar() =>
            ar += java.lang.Long.valueOf(it.size)
          case Max(c) =>
            val i = columnIndex(c)
            var max = java.lang.Integer.MIN_VALUE
            it.foreach { r => if (r(i).asInstanceOf[Int] > max) max = r(i).asInstanceOf[Int] }
            c match {
              case "a" => ar += java.lang.Integer.valueOf(max)
              case "b" => ar += java.lang.Long.valueOf(max)
              case "d" => ar += java.lang.Double.valueOf(max)
              case "e" => ar += java.math.BigDecimal.valueOf(max)
              case "f" => ar += java.lang.Float.valueOf(max)
              case "i" => ar += java.lang.Byte.valueOf(max.toByte)
              case "j" => ar += java.lang.Short.valueOf(max.toShort)
            }
          case Min(c) =>
            val i = columnIndex(c)
            var min = java.lang.Integer.MAX_VALUE
            it.foreach { r => if (r(i).asInstanceOf[Int] < min) min = r(i).asInstanceOf[Int] }
            c match {
              case "a" => ar += java.lang.Integer.valueOf(min)
              case "b" => ar += java.lang.Long.valueOf(min)
              case "d" => ar += java.lang.Double.valueOf(min)
              case "e" => ar += java.math.BigDecimal.valueOf(min)
              case "f" => ar += java.lang.Float.valueOf(min)
              case "i" => ar += java.lang.Byte.valueOf(min.toByte)
              case "j" => ar += java.lang.Short.valueOf(min.toShort)
            }
        }

        (k, ar)
      }

    aggregated.map { case (_, aggResult) =>
      Row.fromSeq(aggResult)
    }

  }

}

class AggregatedScanSuite extends DataSourceTest with SharedSQLContext with PredicateHelper {
  protected override lazy val sql = spark.sql _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.AGGREGATION_PUSHDOWN_ENABLED.key, true)
    sql(
      """
        |CREATE TEMPORARY VIEW oneToTenAggregated
        |USING org.apache.spark.sql.sources.AggregatedScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)
  }

  //////////////////////////////////////////////////
  // COUNT NULLABLE COLUMN
  //////////////////////////////////////////////////
  sqlTest(
    "SELECT count(a) FROM oneToTenAggregated",
    Seq(Row(java.lang.Long.valueOf(9))))

  sqlTest(
    "SELECT count(a) FROM oneToTenAggregated WHERE a IS NULL",
    Seq(Row(java.lang.Long.valueOf(0))))

  sqlTest(
    "SELECT c, count(a) FROM oneToTenAggregated GROUP BY c ORDER BY c",
    Seq(Row("aaaaaAAAAA", java.lang.Long.valueOf(5)),
      Row("bbbbbBBBBB", java.lang.Long.valueOf(4))))


  //////////////////////////////////////////////////
  // EMPTY GROUPING COLUMNS
  //////////////////////////////////////////////////
  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated",
    Seq(Row(java.lang.Long.valueOf(10))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE a IS NOT NULL",
    Seq(Row(java.lang.Long.valueOf(9))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE a IS NULL",
    Seq(Row(java.lang.Long.valueOf(1))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE a = 1",
    Seq(Row(java.lang.Long.valueOf(1))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE a IN (1, 3, 5)",
    Seq(Row(java.lang.Long.valueOf(3))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE a < 5 AND a > 1",
    Seq(Row(java.lang.Long.valueOf(3))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE a < 3 OR a > 8",
    Seq(Row(java.lang.Long.valueOf(4))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE NOT (a < 6)",
    Seq(Row(java.lang.Long.valueOf(4))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE c like 'a%'",
    Seq(Row(java.lang.Long.valueOf(5))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE c like '%B'",
    Seq(Row(java.lang.Long.valueOf(5))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE c like '%aA%'",
    Seq(Row(java.lang.Long.valueOf(5))))

  sqlTest(
    "SELECT sum(a) FROM oneToTenAggregated",
    Seq(Row(java.lang.Long.valueOf((1 to 10).sum))))

  sqlTest(
    "SELECT sum(b) FROM oneToTenAggregated WHERE a IS NOT NULL",
    Seq(Row(java.lang.Double.valueOf((1 to 10).sum - 7))))

  sqlTest(
    "SELECT sum(d) FROM oneToTenAggregated WHERE a IS NULL GROUP BY c",
    Seq(Row(java.lang.Double.valueOf(7))))

  sqlTest(
    "SELECT sum(e) FROM oneToTenAggregated WHERE a = 1",
    Seq(Row(java.math.BigDecimal.valueOf(1))))

  sqlTest(
    "SELECT avg(a) FROM oneToTenAggregated WHERE a IN (1, 3, 5)",
    Seq(Row(java.lang.Double.valueOf(3))))

  sqlTest(
    "SELECT avg(b) FROM oneToTenAggregated WHERE a < 5 AND a > 1",
    Seq(Row(java.lang.Double.valueOf(3))))

  sqlTest(
    "SELECT avg(d) FROM oneToTenAggregated WHERE NOT (a < 6)",
    Seq(Row(java.lang.Double.valueOf((6 + 8 + 9 + 10) / 4d))))

  sqlTest(
    "SELECT avg(e) FROM oneToTenAggregated WHERE a < 3 OR a > 8",
    Seq(Row(java.math.BigDecimal.valueOf(22d / 4))))

  sqlTest(
    "SELECT max(a) FROM oneToTenAggregated WHERE c like 'a%'",
    Seq(Row(java.lang.Integer.valueOf(10))))

  sqlTest(
    "SELECT max(b) FROM oneToTenAggregated WHERE c like '%B'",
    Seq(Row(java.lang.Long.valueOf(9))))

  sqlTest(
    "SELECT min(d) FROM oneToTenAggregated WHERE c like '%aA%'",
    Seq(Row(java.lang.Double.valueOf(2))))

  sqlTest(
    "SELECT min(e) FROM oneToTenAggregated WHERE c like '%bB%'",
    Seq(Row(java.math.BigDecimal.valueOf(1))))


  //////////////////////////////////////////////////
  // ByteType, ShortType, FloatType
  //////////////////////////////////////////////////
  sqlTest(
    "SELECT sum(f) FROM oneToTenAggregated",
    Seq(Row(java.lang.Double.valueOf((1 to 10).sum))))

  sqlTest(
    "SELECT avg(f) FROM oneToTenAggregated",
    Seq(Row(java.lang.Double.valueOf((1 to 10).sum / 10d))))

  sqlTest(
    "SELECT sum(i) FROM oneToTenAggregated",
    Seq(Row(java.lang.Long.valueOf((1 to 10).sum))))

  sqlTest(
    "SELECT avg(i) FROM oneToTenAggregated",
    Seq(Row(java.lang.Double.valueOf((1 to 10).sum / 10d))))

  sqlTest(
    "SELECT sum(j) FROM oneToTenAggregated",
    Seq(Row(java.lang.Long.valueOf((1 to 10).sum))))

  sqlTest(
    "SELECT avg(j) FROM oneToTenAggregated",
    Seq(Row(java.lang.Double.valueOf((1 to 10).sum / 10d))))

  sqlTest(
    "SELECT sum(f) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT avg(f) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5))))

  sqlTest(
    "SELECT sum(i) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT avg(i) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5))))

  sqlTest(
    "SELECT sum(j) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT avg(j) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5))))

  sqlTest(
    "SELECT max(f) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Float.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT min(i) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Byte.valueOf(if (i == 1) 1.toByte else 2.toByte ))))

  sqlTest(
    "SELECT max(j) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Short.valueOf(if (i == 1) 9.toShort else 10.toShort ))))


  //////////////////////////////////////////////////
  // ONE GROUPING COLUMN
  //////////////////////////////////////////////////
  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(java.lang.Long.valueOf(5))))

  sqlTest(
    "SELECT count(e) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(java.lang.Long.valueOf(5))))

  sqlTest(
    "SELECT sum(a) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT sum(b) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT sum(d) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT sum(e) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.math.BigDecimal.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT avg(a) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / (if (i == 1) 4d else 5d)))))

  sqlTest(
    "SELECT avg(b) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5))))

  sqlTest(
    "SELECT avg(d) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5))))

  sqlTest(
    "SELECT avg(e) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.math.BigDecimal.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5d))))

  sqlTest(
    "SELECT max(a) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Integer.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT max(b) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT max(d) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT max(e) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.math.BigDecimal.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT min(a) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Integer.valueOf(if (i == 1) 1 else 2 ))))

  sqlTest(
    "SELECT min(b) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf(if (i == 1) 1 else 2 ))))

  sqlTest(
    "SELECT min(d) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf(if (i == 1) 1 else 2 ))))

  sqlTest(
    "SELECT min(e) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.math.BigDecimal.valueOf(if (i == 1) 1 else 2 ))))


  //////////////////////////////////////////////////
  // SELECT GROUPING COLUMN
  //////////////////////////////////////////////////
  sqlTest(
    "SELECT count(*), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf(5),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT c, sum(a) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5,
      java.lang.Long.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT c, sum(b) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5,
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT c, sum(d) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5,
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT c, sum(e) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5,
      java.math.BigDecimal.valueOf((1 to 10).filter(n => n % 2 == i).sum))))

  sqlTest(
    "SELECT avg(a), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / (if (i == 1) 4d else 5d)),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT avg(b), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT avg(d), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT avg(e), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.math.BigDecimal.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5d),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT c, max(a) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5,
      java.lang.Integer.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT c, max(b) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5,
      java.lang.Long.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT c, max(d) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5,
      java.lang.Double.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT c, max(e) FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5,
      java.math.BigDecimal.valueOf(if (i == 1) 9 else 10 ))))

  sqlTest(
    "SELECT min(a), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Integer.valueOf(if (i == 1) 1 else 2 ),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT min(b), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf(if (i == 1) 1 else 2 ),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT min(d), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Double.valueOf(if (i == 1) 1 else 2 ),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT min(e), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.math.BigDecimal.valueOf(if (i == 1) 1 else 2 ),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT count(d), sum(d), avg(d), c FROM oneToTenAggregated GROUP BY c",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf(5),
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum),
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5d),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))


  //////////////////////////////////////////////////
  // PREDICATE
  //////////////////////////////////////////////////
  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE a IS NOT NULL GROUP BY c ORDER BY c",
    Seq(Row(java.lang.Long.valueOf(5)), Row(java.lang.Long.valueOf(4))))

  sqlTest(
    "SELECT count(*), c FROM oneToTenAggregated WHERE a IS NULL GROUP BY c",
    Seq(Row(java.lang.Long.valueOf(1), "bbbbbBBBBB")))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE a = 1 GROUP BY c",
    Seq(Row(java.lang.Long.valueOf(1))))

  sqlTest(
    "SELECT c, count(*) AS cnt FROM oneToTenAggregated WHERE a IN (1, 3, 6) GROUP BY c " +
      "ORDER BY cnt",
    Seq(Row("aaaaaAAAAA", java.lang.Long.valueOf(1)),
      Row("bbbbbBBBBB", java.lang.Long.valueOf(2))))

  sqlTest(
    "SELECT count(*) AS cnt, c FROM oneToTenAggregated WHERE a < 5 AND a > 1 GROUP BY c " +
      "ORDER BY cnt",
    Seq(Row(java.lang.Long.valueOf(1), "bbbbbBBBBB"),
      Row(java.lang.Long.valueOf(2), "aaaaaAAAAA")))

  sqlTest(
    "SELECT c, count(*) FROM oneToTenAggregated WHERE a < 3 OR a > 8 GROUP BY c ORDER BY c",
    Seq(Row("aaaaaAAAAA", java.lang.Long.valueOf(2)),
      Row("bbbbbBBBBB", java.lang.Long.valueOf(2))))

  sqlTest(
    "SELECT count(*), c FROM oneToTenAggregated WHERE NOT (a < 6) GROUP BY c ORDER BY c DESC",
    Seq(Row(java.lang.Long.valueOf(1), "bbbbbBBBBB"),
      Row(java.lang.Long.valueOf(3), "aaaaaAAAAA")))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE c like 'a%' GROUP BY c",
    Seq(Row(java.lang.Long.valueOf(5))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE c like '%B' GROUP BY c",
    Seq(Row(java.lang.Long.valueOf(5))))

  sqlTest(
    "SELECT count(*) FROM oneToTenAggregated WHERE c like '%aA%' GROUP BY c",
    Seq(Row(java.lang.Long.valueOf(5))))


  //////////////////////////////////////////////////
  // TWO GROUPING COLUMNS
  //////////////////////////////////////////////////
  sqlTest(
    "SELECT count(d), g, c FROM oneToTenAggregated GROUP BY c, g",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf(5),
      java.lang.Integer.valueOf(i),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))

  sqlTest(
    "SELECT count(d), sum(d), avg(d), g, c FROM oneToTenAggregated GROUP BY c, g",
    Seq(1, 0).map(i => Row(
      java.lang.Long.valueOf(5),
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum),
      java.lang.Double.valueOf((1 to 10).filter(n => n % 2 == i).sum / 5d),
      java.lang.Integer.valueOf(i),
      (i + 'a').toChar.toString * 5 + (i + 'a').toChar.toString.toUpperCase * 5)))


  // Dont support pushing down DISTINCT aggregation
  testPlanFailed("SELECT count(DISTINCT c) FROM oneToTenAggregated")

  // Cant pushing down when there are some unhandled filters
  testPlanFailed("SELECT count(*) FROM oneToTenAggregated WHERE b = 1")

  // Cant pushing down unsupported aggregate functions
  testPlanFailed("SELECT first(a) FROM oneToTenAggregated GROUP BY c")

  def testPlanFailed(sql: String): Unit = {
    test(sql) {
      var success = false
      try spark.sql(sql).collect() catch {
        case e: java.lang.AssertionError =>
          // org.apache.spark.sql.catalyst.planning.QueryPlanner.plan failed
          if (e.getMessage.contains("No plan for")) {
            success = true
          }
        case t: Throwable => logWarning(t.toString)
      }
      if (!success) fail
    }
  }

}
