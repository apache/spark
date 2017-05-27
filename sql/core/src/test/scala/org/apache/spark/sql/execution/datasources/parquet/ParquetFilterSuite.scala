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

import java.nio.charset.StandardCharsets

import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators}
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate.Operators.{Column => _, _}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2}

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
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        val query = df
          .select(output.map(e => Column(e)): _*)
          .where(Column(predicate))

        var maybeRelation: Option[HadoopFsRelation] = None
        val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan.collect {
          case PhysicalOperation(_, filters, LogicalRelation(relation: HadoopFsRelation, _, _)) =>
            maybeRelation = Some(relation)
            filters
        }.flatten.reduceLeftOption(_ && _)
        assert(maybeAnalyzedPredicate.isDefined, "No filter is analyzed from the given query")

        val (_, selectedFilters, _) =
          DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate.toSeq)
        assert(selectedFilters.nonEmpty, "No filter is pushed down")

        selectedFilters.foreach { pred =>
          val maybeFilter = ParquetFilters.createFilter(df.schema, pred)
          assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          maybeFilter.exists(_.getClass === filterClass)
        }
        checker(stripSparkFilter(query), expected)
      }
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
        df.rdd.map(_.getAs[Array[Byte]](0).mkString(",")).collect().toSeq.sorted
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
      checkFilterPredicate('_1 =!= true, classOf[NotEq[_]], false)
    }
  }

  test("filter pushdown - integer") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

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

  test("filter pushdown - long") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

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

  test("filter pushdown - float") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

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

  test("filter pushdown - double") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 =!= 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

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

  test("filter pushdown - string") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(i.toString))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 === "1", classOf[Eq[_]], "1")
      checkFilterPredicate('_1 <=> "1", classOf[Eq[_]], "1")
      checkFilterPredicate(
        '_1 =!= "1", classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.toString)))

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

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes(StandardCharsets.UTF_8)
    }

    withParquetDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkBinaryFilterPredicate('_1 === 1.b, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate('_1 <=> 1.b, classOf[Eq[_]], 1.b)

      checkBinaryFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkBinaryFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate(
        '_1 =!= 1.b, classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.b)).toSeq)

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

  test("SPARK-6554: don't push down predicates which reference partition columns") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/part=1"
        (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)

        // If the "part = 1" filter gets pushed down, this query will throw an exception since
        // "part" is not a valid column in the actual Parquet file
        checkAnswer(
          spark.read.parquet(dir.getCanonicalPath).filter("part = 1"),
          (1 to 3).map(i => Row(i, i.toString, 1)))
      }
    }
  }

  test("SPARK-10829: Filter combine partition key and attribute doesn't work in DataSource scan") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/part=1"
        (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)

        // If the "part = 1" filter gets pushed down, this query will throw an exception since
        // "part" is not a valid column in the actual Parquet file
        checkAnswer(
          spark.read.parquet(dir.getCanonicalPath).filter("a > 0 and (part = 0 or a > 1)"),
          (2 to 3).map(i => Row(i, i.toString, 1)))
      }
    }
  }

  test("SPARK-12231: test the filter and empty project in partitioned DataSource scan") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}"
        (1 to 3).map(i => (i, i + 1, i + 2, i + 3)).toDF("a", "b", "c", "d").
          write.partitionBy("a").parquet(path)

        // The filter "a > 1 or b < 2" will not get pushed down, and the projection is empty,
        // this query will throw an exception since the project from combinedFilter expect
        // two projection while the
        val df1 = spark.read.parquet(dir.getCanonicalPath)

        assert(df1.filter("a > 1 or b < 2").count() == 2)
      }
    }
  }

  test("SPARK-12231: test the new projection in partitioned DataSource scan") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}"
        (1 to 3).map(i => (i, i + 1, i + 2, i + 3)).toDF("a", "b", "c", "d").
          write.partitionBy("a").parquet(path)

        // test the generate new projection case
        // when projects != partitionAndNormalColumnProjs

        val df1 = spark.read.parquet(dir.getCanonicalPath)

        checkAnswer(
          df1.filter("a > 1 or b > 2").orderBy("a").selectExpr("a", "b", "c", "d"),
          (2 to 3).map(i => Row(i, i + 1, i + 2, i + 3)))
      }
    }
  }


  test("Filter applied on merged Parquet schema with new column should work") {
    import testImplicits._
    Seq("true", "false").map { vectorized =>
      withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized) {
        withTempPath { dir =>
          val path1 = s"${dir.getCanonicalPath}/table1"
          (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path1)
          val path2 = s"${dir.getCanonicalPath}/table2"
          (1 to 3).map(i => (i, i.toString)).toDF("c", "b").write.parquet(path2)

          // No matter "c = 1" gets pushed down or not, this query should work without exception.
          val df = spark.read.parquet(path1, path2).filter("c = 1").selectExpr("c", "b", "a")
          checkAnswer(
            df,
            Row(1, "1", null))

          val path3 = s"${dir.getCanonicalPath}/table3"
          val dfStruct = sparkContext.parallelize(Seq((1, 1))).toDF("a", "b")
          dfStruct.select(struct("a").as("s")).write.parquet(path3)

          val path4 = s"${dir.getCanonicalPath}/table4"
          val dfStruct2 = sparkContext.parallelize(Seq((1, 1))).toDF("c", "b")
          dfStruct2.select(struct("c").as("s")).write.parquet(path4)

          // No matter "s.c = 1" gets pushed down or not, this query should work without exception.
          val dfStruct3 = spark.read.parquet(path3, path4).filter("s.c = 1")
            .selectExpr("s")
          checkAnswer(dfStruct3, Row(Row(null, 1)))
        }
      }
    }
  }

  // The unsafe row RecordReader does not support row by row filtering so run it with it disabled.
  test("SPARK-11661 Still pushdown filters returned by unhandledFilters") {
    import testImplicits._
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        withTempPath { dir =>
          val path = s"${dir.getCanonicalPath}/part=1"
          (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)
          val df = spark.read.parquet(path).filter("a = 2")

          // The result should be single row.
          // When a filter is pushed to Parquet, Parquet can apply it to every row.
          // So, we can check the number of rows returned from the Parquet
          // to make sure our filter pushdown work.
          assert(stripSparkFilter(df).count == 1)
        }
      }
    }
  }

  test("SPARK-12218: 'Not' is included in Parquet filter pushdown") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/table1"
        (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b").write.parquet(path)

        checkAnswer(
          spark.read.parquet(path).where("not (a = 2) or not(b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))

        checkAnswer(
          spark.read.parquet(path).where("not (a = 2 and b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))
      }
    }
  }

  test("SPARK-12218 Converting conjunctions into Parquet filter predicates") {
    val schema = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType, nullable = true),
      StructField("c", DoubleType, nullable = true)
    ))

    assertResult(Some(and(
      lt(intColumn("a"), 10: Integer),
      gt(doubleColumn("c"), 1.5: java.lang.Double)))
    ) {
      ParquetFilters.createFilter(
        schema,
        sources.And(
          sources.LessThan("a", 10),
          sources.GreaterThan("c", 1.5D)))
    }

    assertResult(None) {
      ParquetFilters.createFilter(
        schema,
        sources.And(
          sources.LessThan("a", 10),
          sources.StringContains("b", "prefix")))
    }

    assertResult(None) {
      ParquetFilters.createFilter(
        schema,
        sources.Not(
          sources.And(
            sources.GreaterThan("a", 1),
            sources.StringContains("b", "prefix"))))
    }
  }

  test("SPARK-16371 Do not push down filters when inner name and outer name are the same") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Tuple1(i)))) { implicit df =>
      // Here the schema becomes as below:
      //
      // root
      //  |-- _1: struct (nullable = true)
      //  |    |-- _1: integer (nullable = true)
      //
      // The inner column name, `_1` and outer column name `_1` are the same.
      // Obviously this should not push down filters because the outer column is struct.
      assert(df.filter("_1 IS NOT NULL").count() === 4)
    }
  }

  test("Fiters should be pushed down for vectorized Parquet reader at row group level") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/table"
        (1 to 1024).map(i => (101, i)).toDF("a", "b").write.parquet(path)

        Seq(true, false).foreach { enablePushDown =>
          withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> enablePushDown.toString) {
            val accu = new NumRowGroupsAcc
            sparkContext.register(accu)

            val df = spark.read.parquet(path).filter("a < 100")
            df.foreachPartition(_.foreach(v => accu.add(0)))
            df.collect

            if (enablePushDown) {
              assert(accu.value == 0)
            } else {
              assert(accu.value > 0)
            }
            AccumulatorContext.remove(accu.id)
          }
        }
      }
    }
  }

  test("SPARK-17213: Broken Parquet filter push-down for string columns") {
    withTempPath { dir =>
      import testImplicits._

      val path = dir.getCanonicalPath
      // scalastyle:off nonascii
      Seq("a", "é").toDF("name").write.parquet(path)
      // scalastyle:on nonascii

      assert(spark.read.parquet(path).where("name > 'a'").count() == 1)
      assert(spark.read.parquet(path).where("name >= 'a'").count() == 2)

      // scalastyle:off nonascii
      assert(spark.read.parquet(path).where("name < 'é'").count() == 1)
      assert(spark.read.parquet(path).where("name <= 'é'").count() == 2)
      // scalastyle:on nonascii
    }
  }

  test("SPARK-20364: Disable Parquet predicate pushdown for fields having dots in the names") {
    import testImplicits._

    Seq(true, false).foreach { vectorized =>
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized.toString,
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString) {
        withTempPath { path =>
          Seq(Some(1), None).toDF("col.dots").write.parquet(path.getAbsolutePath)
          val readBack = spark.read.parquet(path.getAbsolutePath).where("`col.dots` IS NOT NULL")
          assert(readBack.count() == 1)
        }
      }
    }
  }
}

class NumRowGroupsAcc extends AccumulatorV2[Integer, Integer] {
  private var _sum = 0

  override def isZero: Boolean = _sum == 0

  override def copy(): AccumulatorV2[Integer, Integer] = {
    val acc = new NumRowGroupsAcc()
    acc._sum = _sum
    acc
  }

  override def reset(): Unit = _sum = 0

  override def add(v: Integer): Unit = _sum += v

  override def merge(other: AccumulatorV2[Integer, Integer]): Unit = other match {
    case a: NumRowGroupsAcc => _sum += a._sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Integer = _sum
}
