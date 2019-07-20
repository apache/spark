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

import scala.util.Random

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax
import org.scalatest.Matchers._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class ObjectHashAggregateSuite
  extends QueryTest
  with SQLTestUtils
  with TestHiveSingleton
  with ExpressionEvalHelper {

  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TEMPORARY FUNCTION hive_max AS '${classOf[GenericUDAFMax].getName}'")
  }

  protected override def afterAll(): Unit = {
    try {
      sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_max")
    } finally {
      super.afterAll()
    }
  }

  test("typed_count without grouping keys") {
    val df = Seq((1: Integer, 2), (null, 2), (3: Integer, 4)).toDF("a", "b")

    checkAnswer(
      df.coalesce(1).select(typed_count($"a")),
      Seq(Row(2))
    )
  }

  test("typed_count without grouping keys and empty input") {
    val df = Seq.empty[(Integer, Int)].toDF("a", "b")

    checkAnswer(
      df.coalesce(1).select(typed_count($"a")),
      Seq(Row(0))
    )
  }

  test("typed_count with grouping keys") {
    val df = Seq((1: Integer, 1), (null, 1), (2: Integer, 2)).toDF("a", "b")

    checkAnswer(
      df.coalesce(1).groupBy($"b").agg(typed_count($"a")),
      Seq(
        Row(1, 1),
        Row(2, 1))
    )
  }

  test("typed_count fallback to sort-based aggregation") {
    withSQLConf(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "2") {
      val df = Seq(
        (null, 1),
        (null, 1),
        (1: Integer, 1),
        (2: Integer, 2),
        (2: Integer, 2),
        (2: Integer, 2)
      ).toDF("a", "b")

      checkAnswer(
        df.coalesce(1).groupBy($"b").agg(typed_count($"a")),
        Seq(Row(1, 1), Row(2, 3))
      )
    }
  }

  test("random input data types") {
    val dataTypes = Seq(
      // Integral types
      ByteType, ShortType, IntegerType, LongType,

      // Fractional types
      FloatType, DoubleType,

      // Decimal types
      DecimalType(25, 5), DecimalType(6, 5),

      // Datetime types
      DateType, TimestampType,

      // Complex types
      ArrayType(IntegerType),
      MapType(DoubleType, LongType),
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", ArrayType(BooleanType), nullable = true),

      // UDT
      new TestUDT.MyDenseVectorUDT(),

      // Others
      StringType,
      BinaryType, NullType, BooleanType
    )

    dataTypes.sliding(2, 1).map(_.toSeq).foreach { dataTypes =>
      // Schema used to generate random input data.
      val schemaForGenerator = StructType(dataTypes.zipWithIndex.map {
        case (fieldType, index) =>
          StructField(s"col_$index", fieldType, nullable = true)
      })

      // Schema of the DataFrame to be tested.
      val schema = StructType(
        StructField("id", IntegerType, nullable = false) +: schemaForGenerator.fields
      )

      logInfo(s"Testing schema:\n${schema.treeString}")

      // Creates a DataFrame for the schema with random data.
      val data = generateRandomRows(schemaForGenerator)
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
      val aggFunctions = schema.fieldNames.map(f => typed_count(col(f)))

      checkAnswer(
        df.agg(aggFunctions.head, aggFunctions.tail: _*),
        Row.fromSeq(data.map(_.toSeq).transpose.map(_.count(_ != null): Long))
      )

      checkAnswer(
        df.groupBy($"id" % 4 as 'mod).agg(aggFunctions.head, aggFunctions.tail: _*),
        data.groupBy(_.getInt(0) % 4).map { case (key, value) =>
          key -> Row.fromSeq(value.map(_.toSeq).transpose.map(_.count(_ != null): Long))
        }.toSeq.map {
          case (key, value) => Row.fromSeq(key +: value.toSeq)
        }
      )

      withSQLConf(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "5") {
        checkAnswer(
          df.agg(aggFunctions.head, aggFunctions.tail: _*),
          Row.fromSeq(data.map(_.toSeq).transpose.map(_.count(_ != null): Long))
        )
      }
    }
  }

  private def percentile_approx(
      column: Column, percentage: Double, isDistinct: Boolean = false): Column = {
    val approxPercentile = new ApproximatePercentile(column.expr, Literal(percentage))
    Column(approxPercentile.toAggregateExpression(isDistinct))
  }

  private def typed_count(column: Column): Column =
    Column(TestingTypedCount(column.expr).toAggregateExpression())

  // Generates 50 random rows for a given schema.
  private def generateRandomRows(schemaForGenerator: StructType): Seq[Row] = {
    val dataGenerator = RandomDataGenerator.forType(
      dataType = schemaForGenerator,
      nullable = true,
      new Random(System.nanoTime())
    ).getOrElse {
      fail(s"Failed to create data generator for schema $schemaForGenerator")
    }

    (1 to 50).map { i =>
      dataGenerator() match {
        case row: Row => Row.fromSeq(i +: row.toSeq)
        case null => Row.fromSeq(i +: Seq.fill(schemaForGenerator.length)(null))
        case other => fail(
          s"Row or null is expected to be generated, " +
            s"but a ${other.getClass.getCanonicalName} is generated."
        )
      }
    }
  }

  makeRandomizedTests()

  private def makeRandomizedTests(): Unit = {
    // A TypedImperativeAggregate function
    val typed = percentile_approx($"c0", 0.5)

    // A Spark SQL native aggregate function with partial aggregation support that can be executed
    // by the Tungsten `HashAggregateExec`
    val withPartialUnsafe = max($"c1")

    // A Spark SQL native aggregate function with partial aggregation support that can only be
    // executed by the Tungsten `HashAggregateExec`
    val withPartialSafe = max($"c2")

    // A Spark SQL native distinct aggregate function
    val withDistinct = countDistinct($"c3")

    val allAggs = Seq(
      "typed" -> typed,
      "with partial + unsafe" -> withPartialUnsafe,
      "with partial + safe" -> withPartialSafe,
      "with distinct" -> withDistinct
    )

    val builtinNumericTypes = Seq(
      // Integral types
      ByteType, ShortType, IntegerType, LongType,

      // Fractional types
      FloatType, DoubleType
    )

    val numericTypes = builtinNumericTypes ++ Seq(
      // Decimal types
      DecimalType(25, 5), DecimalType(6, 5)
    )

    val dateTimeTypes = Seq(DateType, TimestampType)

    val arrayType = ArrayType(IntegerType)

    val structType = new StructType()
      .add("f1", FloatType, nullable = true)
      .add("f2", ArrayType(BooleanType), nullable = true)

    val mapType = MapType(DoubleType, LongType)

    val complexTypes = Seq(arrayType, mapType, structType)

    val orderedComplexType = Seq(arrayType, structType)

    val orderedTypes = numericTypes ++ dateTimeTypes ++ orderedComplexType ++ Seq(
      StringType, BinaryType, NullType, BooleanType
    )

    val udt = new TestUDT.MyDenseVectorUDT()

    val fixedLengthTypes = builtinNumericTypes ++ Seq(BooleanType, NullType)

    val varLenTypes = complexTypes ++ Seq(StringType, BinaryType, udt)

    val varLenOrderedTypes = varLenTypes.intersect(orderedTypes)

    val allTypes = orderedTypes :+ udt

    val seed = System.nanoTime()
    val random = new Random(seed)

    logInfo(s"Using random seed $seed")

    // Generates a random schema for the randomized data generator
    val schema = new StructType()
      .add("c0", numericTypes(random.nextInt(numericTypes.length)), nullable = true)
      .add("c1", fixedLengthTypes(random.nextInt(fixedLengthTypes.length)), nullable = true)
      .add("c2", varLenOrderedTypes(random.nextInt(varLenOrderedTypes.length)), nullable = true)
      .add("c3", allTypes(random.nextInt(allTypes.length)), nullable = true)

    logInfo(
      s"""Using the following random schema to generate all the randomized aggregation tests:
         |
         |${schema.treeString}
       """.stripMargin
    )

    // Builds a randomly generated DataFrame
    val schemaWithId = StructType(StructField("id", IntegerType, nullable = false) +: schema.fields)
    val data = generateRandomRows(schema)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schemaWithId)

    // Tests all combinations of length 1 to 5 types of aggregate functions
    (1 to allAggs.length) foreach { i =>
      allAggs.combinations(i) foreach { targetAggs =>
        val (names, aggs) = targetAggs.unzip

        // Tests aggregation of w/ and w/o grouping keys
        Seq(true, false).foreach { withGroupingKeys =>

          // Tests aggregation with empty and non-empty input rows
          Seq(true, false).foreach { emptyInput =>

            // Builds the aggregation to be tested according to different configurations
            def doAggregation(df: DataFrame): DataFrame = {
              val baseDf = if (emptyInput) {
                val emptyRows = spark.sparkContext.parallelize(Seq.empty[Row], 1)
                spark.createDataFrame(emptyRows, schemaWithId)
              } else {
                df
              }

              if (withGroupingKeys) {
                baseDf
                  .groupBy($"id" % 10 as "group")
                  .agg(aggs.head, aggs.tail: _*)
                  .orderBy("group")
              } else {
                baseDf.agg(aggs.head, aggs.tail: _*)
              }
            }

            // Currently Spark SQL doesn't support evaluating distinct aggregate function together
            // with aggregate functions without partial aggregation support.
            test(
              s"randomized aggregation test - " +
                s"${names.mkString("[", ", ", "]")} - " +
                s"${if (withGroupingKeys) "with" else "without"} grouping keys - " +
                s"with ${if (emptyInput) "empty" else "non-empty"} input"
            ) {
              var expected: Seq[Row] = null
              var actual1: Seq[Row] = null
              var actual2: Seq[Row] = null

              // Disables `ObjectHashAggregateExec` to obtain a standard answer
              withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
                val aggDf = doAggregation(df)

                if (aggs.intersect(Seq(withPartialSafe, typed)).nonEmpty) {
                  assert(containsSortAggregateExec(aggDf))
                  assert(!containsObjectHashAggregateExec(aggDf))
                  assert(!containsHashAggregateExec(aggDf))
                } else {
                  assert(!containsSortAggregateExec(aggDf))
                  assert(!containsObjectHashAggregateExec(aggDf))
                  assert(containsHashAggregateExec(aggDf))
                }

                expected = aggDf.collect().toSeq
              }

              // Enables `ObjectHashAggregateExec`
              withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
                val aggDf = doAggregation(df)

                if (aggs.contains(typed)) {
                  assert(!containsSortAggregateExec(aggDf))
                  assert(containsObjectHashAggregateExec(aggDf))
                  assert(!containsHashAggregateExec(aggDf))
                } else if (aggs.contains(withPartialSafe)) {
                  assert(containsSortAggregateExec(aggDf))
                  assert(!containsObjectHashAggregateExec(aggDf))
                  assert(!containsHashAggregateExec(aggDf))
                } else {
                  assert(!containsSortAggregateExec(aggDf))
                  assert(!containsObjectHashAggregateExec(aggDf))
                  assert(containsHashAggregateExec(aggDf))
                }

                // Disables sort-based aggregation fallback (we only generate 50 rows, so 100 is
                // big enough) to obtain a result to be checked.
                withSQLConf(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "100") {
                  actual1 = aggDf.collect().toSeq
                }

                // Enables sort-based aggregation fallback to obtain another result to be checked.
                withSQLConf(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "3") {
                  // Here we are not reusing `aggDf` because the physical plan in `aggDf` is
                  // cached and won't be re-planned using the new fallback threshold.
                  actual2 = doAggregation(df).collect().toSeq
                }
              }

              doubleSafeCheckRows(actual1, expected, 1e-4)
              doubleSafeCheckRows(actual2, expected, 1e-4)
            }
          }
        }
      }
    }
  }

  private def containsSortAggregateExec(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collectFirst {
      case _: SortAggregateExec => ()
    }.nonEmpty
  }

  private def containsObjectHashAggregateExec(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collectFirst {
      case _: ObjectHashAggregateExec => ()
    }.nonEmpty
  }

  private def containsHashAggregateExec(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collectFirst {
      case _: HashAggregateExec => ()
    }.nonEmpty
  }

  private def doubleSafeCheckRows(actual: Seq[Row], expected: Seq[Row], tolerance: Double): Unit = {
    assert(actual.length == expected.length)
    actual.zip(expected).foreach { case (lhs: Row, rhs: Row) =>
      assert(lhs.length == rhs.length)
      lhs.toSeq.zip(rhs.toSeq).foreach {
        case (a: Double, b: Double) => checkResult(a, b +- tolerance, DoubleType, false)
        case (a, b) => a == b
      }
    }
  }

  test("SPARK-18403 Fix unsafe data false sharing issue in ObjectHashAggregateExec") {
    // SPARK-18403: An unsafe data false sharing issue may trigger OOM / SIGSEGV when evaluating
    // certain aggregate functions. To reproduce this issue, the following conditions must be
    // met:
    //
    //  1. The aggregation must be evaluated using `ObjectHashAggregateExec`;
    //  2. There must be an input column whose data type involves `ArrayType` or `MapType`;
    //  3. Sort-based aggregation fallback must be triggered during evaluation.
    withSQLConf(
      SQLConf.USE_OBJECT_HASH_AGG.key -> "true",
      SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "1"
    ) {
      checkAnswer(
        Seq
          .fill(2)(Tuple1(Array.empty[Int]))
          .toDF("c0")
          .groupBy(lit(1))
          .agg(typed_count($"c0"), max($"c0")),
        Row(1, 2, Array.empty[Int])
      )

      checkAnswer(
        Seq
          .fill(2)(Tuple1(Map.empty[Int, Int]))
          .toDF("c0")
          .groupBy(lit(1))
          .agg(typed_count($"c0"), first($"c0")),
        Row(1, 2, Map.empty[Int, Int])
      )
    }
  }
}
