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
import org.apache.spark.sql.catalyst.expressions.{Literal, Predicate, Row}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.{QueryTest, SQLConf, SchemaRDD}

/**
 * A test suite that tests Parquet filter2 API based filter pushdown optimization.
 *
 * Notice that `!(a cmp b)` are always transformed to its negated form `a cmp' b` by the
 * `BooleanSimplification` optimization rule whenever possible. As a result, predicate `!(a < 1)`
 * results a `GtEq` filter predicate rather than a `Not`.
 *
 * @todo Add test cases for `IsNull` and `IsNotNull` after merging PR #3367
 */
class ParquetFilterSuite extends QueryTest with ParquetTest {
  val sqlContext = TestSQLContext

  private def checkFilterPushdown(
      rdd: SchemaRDD,
      output: Seq[Symbol],
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      checker: (SchemaRDD, Any) => Unit,
      expectedResult: => Any): Unit = {
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED -> "true") {
      val query = rdd.select(output.map(_.attr): _*).where(predicate)

      val maybeAnalyzedPredicate = query.queryExecution.executedPlan.collect {
        case plan: ParquetTableScan => plan.columnPruningPred
      }.flatten.reduceOption(_ && _)

      assert(maybeAnalyzedPredicate.isDefined)
      maybeAnalyzedPredicate.foreach { pred =>
        val maybeFilter = ParquetFilters.createFilter(pred)
        assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
        maybeFilter.foreach(f => assert(f.getClass === filterClass))
      }

      checker(query, expectedResult)
    }
  }

  private def checkFilterPushdown
      (rdd: SchemaRDD, output: Symbol*)
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate])
      (expectedResult: => Any): Unit = {
    checkFilterPushdown(rdd, output, predicate, filterClass, checkAnswer _, expectedResult)
  }

  def checkBinaryFilterPushdown
      (rdd: SchemaRDD, output: Symbol*)
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate])
      (expectedResult: => Any): Unit = {
    def checkBinaryAnswer(rdd: SchemaRDD, result: Any): Unit = {
      val actual = rdd.map(_.getAs[Array[Byte]](0).mkString(",")).collect().toSeq
      val expected = result match {
        case s: Seq[_] => s.map(_.asInstanceOf[Row].getAs[Array[Byte]](0).mkString(","))
        case s => Seq(s.asInstanceOf[Array[Byte]].mkString(","))
      }
      assert(actual.sorted === expected.sorted)
    }
    checkFilterPushdown(rdd, output, predicate, filterClass, checkBinaryAnswer _, expectedResult)
  }

  test("filter pushdown - boolean") {
    withParquetRDD((true :: false :: Nil).map(Tuple1.apply)) { rdd =>
      checkFilterPushdown(rdd, '_1)('_1 === true, classOf[Eq[java.lang.Boolean]])(true)
      checkFilterPushdown(rdd, '_1)('_1 !== true, classOf[Operators.NotEq[java.lang.Boolean]])(false)
    }
  }

  test("filter pushdown - integer") {
    withParquetRDD((1 to 4).map(Tuple1.apply)) { rdd =>
      checkFilterPushdown(rdd, '_1)('_1 === 1, classOf[Eq[Integer]])(1)
      checkFilterPushdown(rdd, '_1)('_1 !== 1, classOf[Operators.NotEq[Integer]]) {
        (2 to 4).map(Row.apply(_))
      }

      checkFilterPushdown(rdd, '_1)('_1 < 2,  classOf[Lt  [Integer]])(1)
      checkFilterPushdown(rdd, '_1)('_1 > 3,  classOf[Gt  [Integer]])(4)
      checkFilterPushdown(rdd, '_1)('_1 <= 1, classOf[LtEq[Integer]])(1)
      checkFilterPushdown(rdd, '_1)('_1 >= 4, classOf[GtEq[Integer]])(4)

      checkFilterPushdown(rdd, '_1)(Literal(1) === '_1, classOf[Eq  [Integer]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(2) >   '_1, classOf[Lt  [Integer]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(3) <   '_1, classOf[Gt  [Integer]])(4)
      checkFilterPushdown(rdd, '_1)(Literal(1) >=  '_1, classOf[LtEq[Integer]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(4) <=  '_1, classOf[GtEq[Integer]])(4)

      checkFilterPushdown(rdd, '_1)(!('_1 < 4), classOf[Operators.GtEq[Integer]])(4)
      checkFilterPushdown(rdd, '_1)('_1 > 2 && '_1 < 4, classOf[Operators.And])(3)
      checkFilterPushdown(rdd, '_1)('_1 < 2 || '_1 > 3, classOf[Operators.Or]) {
        Seq(Row(1), Row(4))
      }
    }
  }

  test("filter pushdown - long") {
    withParquetRDD((1 to 4).map(i => Tuple1(i.toLong))) { rdd =>
      checkFilterPushdown(rdd, '_1)('_1 === 1, classOf[Eq[java.lang.Long]])(1)
      checkFilterPushdown(rdd, '_1)('_1 !== 1, classOf[Operators.NotEq[java.lang.Long]]) {
        (2 to 4).map(Row.apply(_))
      }

      checkFilterPushdown(rdd, '_1)('_1 <  2, classOf[Lt  [java.lang.Long]])(1)
      checkFilterPushdown(rdd, '_1)('_1 >  3, classOf[Gt  [java.lang.Long]])(4)
      checkFilterPushdown(rdd, '_1)('_1 <= 1, classOf[LtEq[java.lang.Long]])(1)
      checkFilterPushdown(rdd, '_1)('_1 >= 4, classOf[GtEq[java.lang.Long]])(4)

      checkFilterPushdown(rdd, '_1)(Literal(1) === '_1, classOf[Eq  [Integer]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(2) >   '_1, classOf[Lt  [java.lang.Long]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(3) <   '_1, classOf[Gt  [java.lang.Long]])(4)
      checkFilterPushdown(rdd, '_1)(Literal(1) >=  '_1, classOf[LtEq[java.lang.Long]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(4) <=  '_1, classOf[GtEq[java.lang.Long]])(4)

      checkFilterPushdown(rdd, '_1)(!('_1 < 4), classOf[Operators.GtEq[java.lang.Long]])(4)
      checkFilterPushdown(rdd, '_1)('_1 > 2 && '_1 < 4, classOf[Operators.And])(3)
      checkFilterPushdown(rdd, '_1)('_1 < 2 || '_1 > 3, classOf[Operators.Or]) {
        Seq(Row(1), Row(4))
      }
    }
  }

  test("filter pushdown - float") {
    withParquetRDD((1 to 4).map(i => Tuple1(i.toFloat))) { rdd =>
      checkFilterPushdown(rdd, '_1)('_1 === 1, classOf[Eq[java.lang.Float]])(1)
      checkFilterPushdown(rdd, '_1)('_1 !== 1, classOf[Operators.NotEq[java.lang.Float]]) {
        (2 to 4).map(Row.apply(_))
      }

      checkFilterPushdown(rdd, '_1)('_1 <  2, classOf[Lt  [java.lang.Float]])(1)
      checkFilterPushdown(rdd, '_1)('_1 >  3, classOf[Gt  [java.lang.Float]])(4)
      checkFilterPushdown(rdd, '_1)('_1 <= 1, classOf[LtEq[java.lang.Float]])(1)
      checkFilterPushdown(rdd, '_1)('_1 >= 4, classOf[GtEq[java.lang.Float]])(4)

      checkFilterPushdown(rdd, '_1)(Literal(1) === '_1, classOf[Eq  [Integer]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(2) >   '_1, classOf[Lt  [java.lang.Float]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(3) <   '_1, classOf[Gt  [java.lang.Float]])(4)
      checkFilterPushdown(rdd, '_1)(Literal(1) >=  '_1, classOf[LtEq[java.lang.Float]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(4) <=  '_1, classOf[GtEq[java.lang.Float]])(4)

      checkFilterPushdown(rdd, '_1)(!('_1 < 4), classOf[Operators.GtEq[java.lang.Float]])(4)
      checkFilterPushdown(rdd, '_1)('_1 > 2 && '_1 < 4, classOf[Operators.And])(3)
      checkFilterPushdown(rdd, '_1)('_1 < 2 || '_1 > 3, classOf[Operators.Or]) {
        Seq(Row(1), Row(4))
      }
    }
  }

  test("filter pushdown - double") {
    withParquetRDD((1 to 4).map(i => Tuple1(i.toDouble))) { rdd =>
      checkFilterPushdown(rdd, '_1)('_1 === 1, classOf[Eq[java.lang.Double]])(1)
      checkFilterPushdown(rdd, '_1)('_1 !== 1, classOf[Operators.NotEq[java.lang.Double]]) {
        (2 to 4).map(Row.apply(_))
      }

      checkFilterPushdown(rdd, '_1)('_1 <  2, classOf[Lt  [java.lang.Double]])(1)
      checkFilterPushdown(rdd, '_1)('_1 >  3, classOf[Gt  [java.lang.Double]])(4)
      checkFilterPushdown(rdd, '_1)('_1 <= 1, classOf[LtEq[java.lang.Double]])(1)
      checkFilterPushdown(rdd, '_1)('_1 >= 4, classOf[GtEq[java.lang.Double]])(4)

      checkFilterPushdown(rdd, '_1)(Literal(1) === '_1, classOf[Eq[Integer]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(2) >   '_1, classOf[Lt  [java.lang.Double]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(3) <   '_1, classOf[Gt  [java.lang.Double]])(4)
      checkFilterPushdown(rdd, '_1)(Literal(1) >=  '_1, classOf[LtEq[java.lang.Double]])(1)
      checkFilterPushdown(rdd, '_1)(Literal(4) <=  '_1, classOf[GtEq[java.lang.Double]])(4)

      checkFilterPushdown(rdd, '_1)(!('_1 < 4), classOf[Operators.GtEq[java.lang.Double]])(4)
      checkFilterPushdown(rdd, '_1)('_1 > 2 && '_1 < 4, classOf[Operators.And])(3)
      checkFilterPushdown(rdd, '_1)('_1 < 2 || '_1 > 3, classOf[Operators.Or]) {
        Seq(Row(1), Row(4))
      }
    }
  }

  test("filter pushdown - string") {
    withParquetRDD((1 to 4).map(i => Tuple1(i.toString))) { rdd =>
      checkFilterPushdown(rdd, '_1)('_1 === "1", classOf[Eq[String]])("1")
      checkFilterPushdown(rdd, '_1)('_1 !== "1", classOf[Operators.NotEq[String]]) {
        (2 to 4).map(i => Row.apply(i.toString))
      }

      checkFilterPushdown(rdd, '_1)('_1 <  "2", classOf[Lt  [java.lang.String]])("1")
      checkFilterPushdown(rdd, '_1)('_1 >  "3", classOf[Gt  [java.lang.String]])("4")
      checkFilterPushdown(rdd, '_1)('_1 <= "1", classOf[LtEq[java.lang.String]])("1")
      checkFilterPushdown(rdd, '_1)('_1 >= "4", classOf[GtEq[java.lang.String]])("4")

      checkFilterPushdown(rdd, '_1)(Literal("1") === '_1, classOf[Eq  [java.lang.String]])("1")
      checkFilterPushdown(rdd, '_1)(Literal("2") >   '_1, classOf[Lt  [java.lang.String]])("1")
      checkFilterPushdown(rdd, '_1)(Literal("3") <   '_1, classOf[Gt  [java.lang.String]])("4")
      checkFilterPushdown(rdd, '_1)(Literal("1") >=  '_1, classOf[LtEq[java.lang.String]])("1")
      checkFilterPushdown(rdd, '_1)(Literal("4") <=  '_1, classOf[GtEq[java.lang.String]])("4")

      checkFilterPushdown(rdd, '_1)(!('_1 < "4"), classOf[Operators.GtEq[java.lang.String]])("4")
      checkFilterPushdown(rdd, '_1)('_1 > "2" && '_1 < "4", classOf[Operators.And])("3")
      checkFilterPushdown(rdd, '_1)('_1 < "2" || '_1 > "3", classOf[Operators.Or]) {
        Seq(Row("1"), Row("4"))
      }
    }
  }

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes("UTF-8")
    }

    withParquetRDD((1 to 4).map(i => Tuple1(i.b))) { rdd =>
      checkBinaryFilterPushdown(rdd, '_1)('_1 === 1.b, classOf[Eq[Array[Byte]]])(1.b)
      checkBinaryFilterPushdown(rdd, '_1)('_1 !== 1.b, classOf[Operators.NotEq[Array[Byte]]]) {
        (2 to 4).map(i => Row.apply(i.b)).toSeq
      }

      checkBinaryFilterPushdown(rdd, '_1)('_1 <  2.b, classOf[Lt  [Array[Byte]]])(1.b)
      checkBinaryFilterPushdown(rdd, '_1)('_1 >  3.b, classOf[Gt  [Array[Byte]]])(4.b)
      checkBinaryFilterPushdown(rdd, '_1)('_1 <= 1.b, classOf[LtEq[Array[Byte]]])(1.b)
      checkBinaryFilterPushdown(rdd, '_1)('_1 >= 4.b, classOf[GtEq[Array[Byte]]])(4.b)

      checkBinaryFilterPushdown(rdd, '_1)(Literal(1.b) === '_1, classOf[Eq  [Array[Byte]]])(1.b)
      checkBinaryFilterPushdown(rdd, '_1)(Literal(2.b) >   '_1, classOf[Lt  [Array[Byte]]])(1.b)
      checkBinaryFilterPushdown(rdd, '_1)(Literal(3.b) <   '_1, classOf[Gt  [Array[Byte]]])(4.b)
      checkBinaryFilterPushdown(rdd, '_1)(Literal(1.b) >=  '_1, classOf[LtEq[Array[Byte]]])(1.b)
      checkBinaryFilterPushdown(rdd, '_1)(Literal(4.b) <=  '_1, classOf[GtEq[Array[Byte]]])(4.b)

      checkBinaryFilterPushdown(rdd, '_1)(!('_1 < 4.b), classOf[Operators.GtEq[Array[Byte]]])(4.b)
      checkBinaryFilterPushdown(rdd, '_1)('_1 > 2.b && '_1 < 4.b, classOf[Operators.And])(3.b)
      checkBinaryFilterPushdown(rdd, '_1)('_1 < 2.b || '_1 > 3.b, classOf[Operators.Or]) {
        Seq(Row(1.b), Row(4.b))
      }
    }
  }
}
