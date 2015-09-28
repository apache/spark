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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.filter2.predicate.Operators._
import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators}
import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.{Column, DataFrame, QueryTest, Row, SQLConf}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

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
class ParquetFilterSuite extends QueryTest with ParquetTest with SharedSQLContext {

  private def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      checker: (DataFrame, Seq[Row]) => Unit,
      expected: Seq[Row]): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      val query = df
        .select(output.map(e => Column(e)): _*)
        .where(Column(predicate))

      val analyzedPredicate = query.queryExecution.optimizedPlan.collect {
        case PhysicalOperation(_, filters, LogicalRelation(_: ParquetRelation)) => filters
      }.flatten
      assert(analyzedPredicate.nonEmpty)

      val selectedFilters = DataSourceStrategy.selectFilters(analyzedPredicate)
      assert(selectedFilters.nonEmpty)

      selectedFilters.foreach { pred =>
        val maybeFilter = ParquetFilters.createFilter(df.schema, pred)
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
      (implicit df: DataFrame): Unit = {
    checkFilterPredicate(df, predicate, filterClass, checkAnswer(_, _: Seq[Row]), expected)
  }

  private def checkFilterPredicate[T]
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: T)
      (implicit df: DataFrame): Unit = {
    checkFilterPredicate(predicate, filterClass, Seq(Row(expected)))(df)
  }

  private def checkBinaryFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Seq[Row])
      (implicit df: DataFrame): Unit = {
    def checkBinaryAnswer(df: DataFrame, expected: Seq[Row]) = {
      assertResult(expected.map(_.getAs[Array[Byte]](0).mkString(",")).sorted) {
        df.map(_.getAs[Array[Byte]](0).mkString(",")).collect().toSeq.sorted
      }
    }

    checkFilterPredicate(df, predicate, filterClass, checkBinaryAnswer _, expected)
  }

  private def checkBinaryFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Array[Byte])
      (implicit df: DataFrame): Unit = {
    checkBinaryFilterPredicate(predicate, filterClass, Seq(Row(expected)))(df)
  }

  test("filter pushdown - boolean") {
    withParquetDataFrame((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], Seq(Row(true), Row(false)))

      checkFilterPredicate('_1 === true, classOf[Eq[_]], true)
      checkFilterPredicate('_1 <=> true, classOf[Eq[_]], true)
      checkFilterPredicate('_1 !== true, classOf[NotEq[_]], false)
    }
  }

  test("filter pushdown - udf on boolean column") {
    withParquetDataFrame((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit df =>
      def booleanToInt: Boolean => Int = (x: Boolean) => if (x) 10 else 20
      sqlContext.udf.register("booleanToInt", booleanToInt)

      val udf = ScalaUDF(booleanToInt, IntegerType, Seq('_1.expr))
      checkFilterPredicate(EqualTo(udf, Literal(10)),
        classOf[UserDefinedByInstance[_, _]], true)
      checkFilterPredicate(EqualTo(udf, Literal(20)),
        classOf[UserDefinedByInstance[_, _]], false)
    }
  }

  test("filter pushdown - integer") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - udf on int column") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      def intToString: Integer => String = (x: Integer) => (x - 1).toString
      sqlContext.udf.register("intToString", intToString)

      val udf = ScalaUDF(intToString, StringType, Seq('_1.expr))
      checkFilterPredicate(EqualTo(udf, Literal("1")),
        classOf[UserDefinedByInstance[_, _]], 2)
      checkFilterPredicate(EqualTo(udf, Literal("2")),
        classOf[UserDefinedByInstance[_, _]], 3)
      checkFilterPredicate(EqualTo(udf, Literal("5")),
        classOf[UserDefinedByInstance[_, _]], Seq.empty[Row])
    }
  }

  test("filter pushdown - long") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - udf on long column") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit df =>
      def longToString: Long => String = (x: Long) => (x * 10).toString
      sqlContext.udf.register("longToString", longToString)

      val udf = ScalaUDF(longToString, StringType, Seq('_1.expr))
      checkFilterPredicate(EqualTo(udf, Literal("10")),
        classOf[UserDefinedByInstance[_, _]], 1)
      checkFilterPredicate(EqualTo(udf, Literal("40")),
        classOf[UserDefinedByInstance[_, _]], 4)
      checkFilterPredicate(EqualTo(udf, Literal("50")),
        classOf[UserDefinedByInstance[_, _]], Seq.empty[Row])
    }
  }

  test("filter pushdown - float") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - udf on float column") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit df =>
      def floatToString: Float => String = (x: Float) => (x + 10.1).toString
      sqlContext.udf.register("floatToString", floatToString)

      val udf = ScalaUDF(floatToString, StringType, Seq('_1.expr))
      checkFilterPredicate(EqualTo(udf, Literal("11.1")),
        classOf[UserDefinedByInstance[_, _]], 1)
      checkFilterPredicate(EqualTo(udf, Literal("14.1")),
        classOf[UserDefinedByInstance[_, _]], 4)
      checkFilterPredicate(EqualTo(udf, Literal("15.1")),
        classOf[UserDefinedByInstance[_, _]], Seq.empty[Row])
    }
  }

  test("filter pushdown - double") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  test("filter pushdown - udf on double column") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit df =>
      def doubleToString: Double => String = (x: Double) => (x * 100.1).toString
      sqlContext.udf.register("doubleToString", doubleToString)

      val udf = ScalaUDF(doubleToString, StringType, Seq('_1.expr))
      checkFilterPredicate(EqualTo(udf, Literal("100.1")),
        classOf[UserDefinedByInstance[_, _]], 1)
      checkFilterPredicate(EqualTo(udf, Literal("400.4")),
        classOf[UserDefinedByInstance[_, _]], 4)
      checkFilterPredicate(EqualTo(udf, Literal("500.5")),
        classOf[UserDefinedByInstance[_, _]], Seq.empty[Row])
    }
  }

  test("filter pushdown - string") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(i.toString))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 === "1", classOf[Eq[_]], "1")
      checkFilterPredicate('_1 <=> "1", classOf[Eq[_]], "1")
      checkFilterPredicate(
        '_1 !== "1", classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 < "2", classOf[Lt[_]], "1")
      checkFilterPredicate('_1 > "3", classOf[Gt[_]], "4")
      checkFilterPredicate('_1 <= "1", classOf[LtEq[_]], "1")
      checkFilterPredicate('_1 >= "4", classOf[GtEq[_]], "4")

      checkFilterPredicate(Literal("1") === '_1, classOf[Eq[_]], "1")
      checkFilterPredicate(Literal("1") <=> '_1, classOf[Eq[_]], "1")
      checkFilterPredicate(Literal("2") > '_1, classOf[Lt[_]], "1")
      checkFilterPredicate(Literal("3") < '_1, classOf[Gt[_]], "4")
      checkFilterPredicate(Literal("1") >= '_1, classOf[LtEq[_]], "1")
      checkFilterPredicate(Literal("4") <= '_1, classOf[GtEq[_]], "4")

      checkFilterPredicate(!('_1 < "4"), classOf[GtEq[_]], "4")
      checkFilterPredicate('_1 < "2" || '_1 > "3", classOf[Operators.Or], Seq(Row("1"), Row("4")))
    }
  }

  test("filter pushdown - udf on string column") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toString)))) { implicit df =>
      def stringToInt: String => Int = (x: String) => x.toInt
      sqlContext.udf.register("stringToInt", stringToInt)

      val udf = ScalaUDF(stringToInt, IntegerType, Seq('_1.expr))
      checkFilterPredicate(EqualTo(udf, Literal(1)),
        classOf[UserDefinedByInstance[_, _]], "1")
      checkFilterPredicate(EqualTo(udf, Literal(4)),
        classOf[UserDefinedByInstance[_, _]], "4")
      checkFilterPredicate(EqualTo(udf, Literal(5)),
        classOf[UserDefinedByInstance[_, _]], Seq.empty[Row])
    }
  }

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes("UTF-8")
    }

    withParquetDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkBinaryFilterPredicate('_1 === 1.b, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate('_1 <=> 1.b, classOf[Eq[_]], 1.b)

      checkBinaryFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkBinaryFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate(
        '_1 !== 1.b, classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate('_1 < 2.b, classOf[Lt[_]], 1.b)
      checkBinaryFilterPredicate('_1 > 3.b, classOf[Gt[_]], 4.b)
      checkBinaryFilterPredicate('_1 <= 1.b, classOf[LtEq[_]], 1.b)
      checkBinaryFilterPredicate('_1 >= 4.b, classOf[GtEq[_]], 4.b)

      checkBinaryFilterPredicate(Literal(1.b) === '_1, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(1.b) <=> '_1, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(2.b) > '_1, classOf[Lt[_]], 1.b)
      checkBinaryFilterPredicate(Literal(3.b) < '_1, classOf[Gt[_]], 4.b)
      checkBinaryFilterPredicate(Literal(1.b) >= '_1, classOf[LtEq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(4.b) <= '_1, classOf[GtEq[_]], 4.b)

      checkBinaryFilterPredicate(!('_1 < 4.b), classOf[GtEq[_]], 4.b)
      checkBinaryFilterPredicate(
        '_1 < 2.b || '_1 > 3.b, classOf[Operators.Or], Seq(Row(1.b), Row(4.b)))
    }
  }

  test("filter pushdown - udf on binary column") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes("UTF-8")
    }

    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.b)))) { implicit df =>
      def binaryToInt: Array[Byte] => Int = (x: Array[Byte]) => new String(x).toInt
      sqlContext.udf.register("binaryToInt", binaryToInt)

      val udf = ScalaUDF(binaryToInt, IntegerType, Seq('_1.expr))
      checkFilterPredicate(EqualTo(udf, Literal(1)),
        classOf[UserDefinedByInstance[_, _]], 1.b)
      checkFilterPredicate(EqualTo(udf, Literal(4)),
        classOf[UserDefinedByInstance[_, _]], 4.b)
      checkFilterPredicate(EqualTo(udf, Literal(5)),
        classOf[UserDefinedByInstance[_, _]], Seq.empty[Row])
    }
  }

  test("SPARK-6554: don't push down predicates which reference partition columns") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/part=1"
        (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)

        // If the "part = 1" filter gets pushed down, this query will throw an exception since
        // "part" is not a valid column in the actual Parquet file
        checkAnswer(
          sqlContext.read.parquet(path).filter("part = 1"),
          (1 to 3).map(i => Row(i, i.toString, 1)))
      }
    }
  }
}
