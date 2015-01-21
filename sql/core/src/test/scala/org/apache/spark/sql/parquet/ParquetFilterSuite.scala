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

package org.apache.spark.sql.parquet

import parquet.filter2.predicate.Operators._
import parquet.filter2.predicate.{FilterPredicate, Operators}

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, Predicate, Row}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.{QueryTest, SQLConf, SchemaRDD}

/**
 * A test suite that tests Parquet filter2 API based filter pushdown optimization.
 *
 * NOTE:
 *
 * 1. `!(a cmp b)` is always transformed to its negated form `a cmp' b` by the
 *    `BooleanSimplification` optimization rule whenever possible. As a result, predicate `!(a < 1)`
 *    results in a `GtEq` filter predicate rather than a `Not`.
 *
 * 2. `Tuple1(Option(x))` is used together with `AnyVal` types like `Int` to ensure the inferred
 *    data type is nullable.
 */
class ParquetFilterSuite extends QueryTest with ParquetTest {
  val sqlContext = TestSQLContext

  private def checkFilterPredicate(
      rdd: SchemaRDD,
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      checker: (SchemaRDD, Seq[Row]) => Unit,
      expected: Seq[Row]): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED -> "true") {
      val query = rdd.select(output: _*).where(predicate)

      val maybeAnalyzedPredicate = query.queryExecution.executedPlan.collect {
        case plan: ParquetTableScan => plan.columnPruningPred
      }.flatten.reduceOption(_ && _)

      assert(maybeAnalyzedPredicate.isDefined)
      maybeAnalyzedPredicate.foreach { pred =>
        val maybeFilter = ParquetFilters.createFilter(pred)
        assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
        maybeFilter.foreach { f =>
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          assert(f.getClass === filterClass)
        }
      }

      checker(query, expected)
    }
  }

  private def checkFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Seq[Row])
      (implicit rdd: SchemaRDD): Unit = {
    checkFilterPredicate(rdd, predicate, filterClass, checkAnswer(_, _: Seq[Row]), expected)
  }

  private def checkFilterPredicate[T]
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: T)
      (implicit rdd: SchemaRDD): Unit = {
    checkFilterPredicate(predicate, filterClass, Seq(Row(expected)))(rdd)
  }

  test("filter pushdown - boolean") {
    withParquetRDD((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit rdd =>
      checkFilterPredicate('_1.isNull,    classOf[Eq   [_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], Seq(Row(true), Row(false)))

      checkFilterPredicate('_1 === true, classOf[Eq   [_]], true)
      checkFilterPredicate('_1 !== true, classOf[NotEq[_]], false)
    }
  }

  test("filter pushdown - integer") {
    withParquetRDD((1 to 4).map(i => Tuple1(Option(i)))) { implicit rdd =>
      checkFilterPredicate('_1.isNull,    classOf[Eq   [_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq   [_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2,  classOf[Lt  [_]], 1)
      checkFilterPredicate('_1 > 3,  classOf[Gt  [_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq  [_]], 1)
      checkFilterPredicate(Literal(2) >   '_1, classOf[Lt  [_]], 1)
      checkFilterPredicate(Literal(3) <   '_1, classOf[Gt  [_]], 4)
      checkFilterPredicate(Literal(1) >=  '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <=  '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4),         classOf[GtEq[_]],       4)
      checkFilterPredicate('_1 > 2 && '_1 < 4, classOf[Operators.And], 3)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or],  Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - long") {
    withParquetRDD((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit rdd =>
      checkFilterPredicate('_1.isNull,    classOf[Eq   [_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]],    1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 <  2, classOf[Lt  [_]], 1)
      checkFilterPredicate('_1 >  3, classOf[Gt  [_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq  [_]], 1)
      checkFilterPredicate(Literal(2) >   '_1, classOf[Lt  [_]], 1)
      checkFilterPredicate(Literal(3) <   '_1, classOf[Gt  [_]], 4)
      checkFilterPredicate(Literal(1) >=  '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <=  '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4),         classOf[GtEq[_]],       4)
      checkFilterPredicate('_1 > 2 && '_1 < 4, classOf[Operators.And], 3)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or],  Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - float") {
    withParquetRDD((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit rdd =>
      checkFilterPredicate('_1.isNull,    classOf[Eq   [_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq   [_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 <  2, classOf[Lt  [_]], 1)
      checkFilterPredicate('_1 >  3, classOf[Gt  [_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq  [_]], 1)
      checkFilterPredicate(Literal(2) >   '_1, classOf[Lt  [_]], 1)
      checkFilterPredicate(Literal(3) <   '_1, classOf[Gt  [_]], 4)
      checkFilterPredicate(Literal(1) >=  '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <=  '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4),         classOf[GtEq[_]],       4)
      checkFilterPredicate('_1 > 2 && '_1 < 4, classOf[Operators.And], 3)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or],  Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - double") {
    withParquetRDD((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit rdd =>
      checkFilterPredicate('_1.isNull,    classOf[Eq[_]],    Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq   [_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 <  2, classOf[Lt  [_]], 1)
      checkFilterPredicate('_1 >  3, classOf[Gt  [_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq  [_]], 1)
      checkFilterPredicate(Literal(2) >   '_1, classOf[Lt  [_]], 1)
      checkFilterPredicate(Literal(3) <   '_1, classOf[Gt  [_]], 4)
      checkFilterPredicate(Literal(1) >=  '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <=  '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4),         classOf[GtEq[_]],       4)
      checkFilterPredicate('_1 > 2 && '_1 < 4, classOf[Operators.And], 3)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or],  Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - string") {
    withParquetRDD((1 to 4).map(i => Tuple1(i.toString))) { implicit rdd =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 === "1", classOf[Eq   [_]], "1")
      checkFilterPredicate('_1 !== "1", classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 <  "2", classOf[Lt  [_]], "1")
      checkFilterPredicate('_1 >  "3", classOf[Gt  [_]], "4")
      checkFilterPredicate('_1 <= "1", classOf[LtEq[_]], "1")
      checkFilterPredicate('_1 >= "4", classOf[GtEq[_]], "4")

      checkFilterPredicate(Literal("1") === '_1, classOf[Eq  [_]], "1")
      checkFilterPredicate(Literal("2") >   '_1, classOf[Lt  [_]], "1")
      checkFilterPredicate(Literal("3") <   '_1, classOf[Gt  [_]], "4")
      checkFilterPredicate(Literal("1") >=  '_1, classOf[LtEq[_]], "1")
      checkFilterPredicate(Literal("4") <=  '_1, classOf[GtEq[_]], "4")

      checkFilterPredicate(!('_1 < "4"),           classOf[GtEq[_]],       "4")
      checkFilterPredicate('_1 > "2" && '_1 < "4", classOf[Operators.And], "3")
      checkFilterPredicate('_1 < "2" || '_1 > "3", classOf[Operators.Or],  Seq(Row("1"), Row("4")))
    }
  }

  def checkBinaryFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Seq[Row])
      (implicit rdd: SchemaRDD): Unit = {
    def checkBinaryAnswer(rdd: SchemaRDD, expected: Seq[Row]) = {
      assertResult(expected.map(_.getAs[Array[Byte]](0).mkString(",")).toSeq.sorted) {
        rdd.map(_.getAs[Array[Byte]](0).mkString(",")).collect().toSeq.sorted
      }
    }

    checkFilterPredicate(rdd, predicate, filterClass, checkBinaryAnswer _, expected)
  }

  def checkBinaryFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Array[Byte])
      (implicit rdd: SchemaRDD): Unit = {
    checkBinaryFilterPredicate(predicate, filterClass, Seq(Row(expected)))(rdd)
  }

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes("UTF-8")
    }

    withParquetRDD((1 to 4).map(i => Tuple1(i.b))) { implicit rdd =>
      checkBinaryFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkBinaryFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate('_1 === 1.b, classOf[Eq   [_]], 1.b)
      checkBinaryFilterPredicate(
        '_1 !== 1.b, classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate('_1 <  2.b, classOf[Lt  [_]], 1.b)
      checkBinaryFilterPredicate('_1 >  3.b, classOf[Gt  [_]], 4.b)
      checkBinaryFilterPredicate('_1 <= 1.b, classOf[LtEq[_]], 1.b)
      checkBinaryFilterPredicate('_1 >= 4.b, classOf[GtEq[_]], 4.b)

      checkBinaryFilterPredicate(Literal(1.b) === '_1, classOf[Eq  [_]], 1.b)
      checkBinaryFilterPredicate(Literal(2.b) >   '_1, classOf[Lt  [_]], 1.b)
      checkBinaryFilterPredicate(Literal(3.b) <   '_1, classOf[Gt  [_]], 4.b)
      checkBinaryFilterPredicate(Literal(1.b) >=  '_1, classOf[LtEq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(4.b) <=  '_1, classOf[GtEq[_]], 4.b)

      checkBinaryFilterPredicate(!('_1 < 4.b), classOf[GtEq[_]], 4.b)
      checkBinaryFilterPredicate('_1 > 2.b && '_1 < 4.b, classOf[Operators.And], 3.b)
      checkBinaryFilterPredicate(
        '_1 < 2.b || '_1 > 3.b, classOf[Operators.Or], Seq(Row(1.b), Row(4.b)))
    }
  }
}
