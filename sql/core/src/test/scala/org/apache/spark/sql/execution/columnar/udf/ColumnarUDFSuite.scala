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

package org.apache.spark.sql.execution.columnar.udf

import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.StoredExpressionFunctions
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, UserDefinedExpression}
import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.columnar.{ColumnarScalarFunction, ColumnarUDFImplementation, ColumnarUserDefinedFunction}
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.MarkSingleTaskExecution
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

/** Adds one to a bigint, and keeps nulls. */
class AddOneFunction extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val in = args.column(0)
    val out = new OnHeapColumnVector(n, LongType)
    var i = 0
    if (in.hasNull) {
      while (i < n) {
        if (in.isNullAt(i)) out.putNull(i) else out.putLong(i, in.getLong(i) + 1)
        i += 1
      }
    } else {
      while (i < n) {
        out.putLong(i, in.getLong(i) + 1)
        i += 1
      }
    }
    out
  }
}

/** Adds two bigints, and returns null if either is null. */
class AddFunction extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val a = args.column(0)
    val b = args.column(1)
    val out = new OnHeapColumnVector(n, LongType)
    var i = 0
    while (i < n) {
      if (a.isNullAt(i) || b.isNullAt(i)) {
        out.putNull(i)
      } else {
        out.putLong(i, a.getLong(i) + b.getLong(i))
      }
      i += 1
    }
    out
  }
}

/** The upper case of a string, followed by an exclamation mark. */
class ShoutFunction extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val in = args.column(0)
    val out = new OnHeapColumnVector(n, StringType)
    var i = 0
    while (i < n) {
      if (in.isNullAt(i)) {
        out.putNull(i)
      } else {
        val upper = in.getUTF8String(i).toString.toUpperCase(Locale.ROOT)
        val bytes = UTF8String.fromString(upper + "!").getBytes
        out.putByteArray(i, bytes)
      }
      i += 1
    }
    out
  }
}

/** Returns the number of values that it has returned so far, which differs for every value. */
class CounterFunction extends ColumnarScalarFunction {
  private val counter = new AtomicLong()

  override def eval(args: ColumnarBatch): ColumnVector = {
    val n = args.numRows()
    val out = new OnHeapColumnVector(n, LongType)
    var i = 0
    while (i < n) {
      out.putLong(i, counter.incrementAndGet())
      i += 1
    }
    out
  }
}

/** Returns its argument column, which a function must not do. */
class ReturnArgumentFunction extends ColumnarScalarFunction {
  override def eval(args: ColumnarBatch): ColumnVector = args.column(0)
}

class ColumnarUDFSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  private val addOne =
    ColumnarUserDefinedFunction("add_one", Seq(LongType), LongType, Some(new AddOneFunction))
  private val add =
    ColumnarUserDefinedFunction("add", Seq(LongType, LongType), LongType, Some(new AddFunction))
  private val shout =
    ColumnarUserDefinedFunction("shout", Seq(StringType), StringType, Some(new ShoutFunction))

  private def evalNodes(plan: SparkPlan): Seq[EvalColumnarUDFsExec] = collect(plan) {
    case e: EvalColumnarUDFsExec => e
  }

  /** Registers the functions for SQL, and drops them afterwards. */
  private def withFunctions(udfs: ColumnarUserDefinedFunction*)(f: => Unit): Unit = {
    withUserDefinedFunction(udfs.map(_.name -> true): _*) {
      udfs.foreach(spark.udf.registerColumnar)
      f
    }
  }

  test("a call over a row-based child") {
    val df = spark.range(5).select(col("id"), addOne(col("id")))
    checkAnswer(df, (0 until 5).map(i => Row(i.toLong, i + 1L)))
    assert(evalNodes(df.queryExecution.executedPlan).length == 1)
  }

  test("a call from SQL, with an argument that is cast") {
    withFunctions(addOne) {
      checkAnswer(sql("SELECT add_one(1), add_one(CAST(NULL AS INT))"), Row(2L, null))
      checkAnswer(
        sql("SELECT sum(add_one(id)) FROM range(1000)"), Row((1 to 1000).map(_.toLong).sum))
    }
  }

  test("a call over a columnar scan has no transition from rows") {
    withTempPath { path =>
      spark.range(100).selectExpr("id", "cast(id as string) as s")
        .write.parquet(path.getCanonicalPath)
      val df = spark.read.parquet(path.getCanonicalPath).select(col("s"), addOne(col("id")))
      checkAnswer(df, (0 until 100).map(i => Row(i.toString, i + 1L)))
      val plan = df.queryExecution.executedPlan
      val eval = evalNodes(plan)
      assert(eval.length == 1)
      assert(collect(plan) { case r: RowToColumnarExec => r }.isEmpty)
    }
  }

  test("sibling calls share a node, and nested calls do not") {
    val siblings = spark.range(5).select(addOne(col("id")), add(col("id"), col("id")))
    checkAnswer(siblings, (0 until 5).map(i => Row(i + 1L, 2L * i)))
    val siblingNodes = evalNodes(siblings.queryExecution.executedPlan)
    assert(siblingNodes.length == 1 && siblingNodes.head.udfs.length == 2)

    val nested = spark.range(5).select(addOne(addOne(col("id"))))
    checkAnswer(nested, (0 until 5).map(i => Row(i + 2L)))
    assert(evalNodes(nested.queryExecution.executedPlan).length == 2)
  }

  test("arguments that are not columns") {
    val df = spark.range(5).select(add(col("id") * 2, lit(10L)), addOne(col("id") * 2))
    checkAnswer(df, (0 until 5).map(i => Row(2L * i + 10, 2L * i + 1)))
  }

  test("a call in a filter, and a filter below a call") {
    val df = spark.range(10).filter(addOne(col("id")) > 5).select(col("id"))
    checkAnswer(df, (5 until 10).map(i => Row(i.toLong)))
    val pushed = spark.range(10).select(col("id"), addOne(col("id")).as("r")).filter("id < 3")
    checkAnswer(pushed, (0 until 3).map(i => Row(i.toLong, i + 1L)))
  }

  test("an argument that is cast is computed by a row-based projection") {
    withTempPath { path =>
      spark.range(100).selectExpr("cast(id as int) as i").write.parquet(path.getCanonicalPath)
      val df = spark.read.parquet(path.getCanonicalPath).select(addOne(col("i")))
      checkAnswer(df, (0 until 100).map(i => Row(i + 1L)))
      val plan = df.queryExecution.executedPlan
      assert(evalNodes(plan).length == 1)
      assert(collect(plan) { case r: RowToColumnarExec => r }.length == 1)
    }
  }

  test("calls in an aggregate that read grouping keys, or no column") {
    withFunctions(addOne) {
      withTempView("t") {
        spark.range(10).selectExpr("id % 3 AS k", "id AS v").createOrReplaceTempView("t")
        checkAnswer(
          sql("SELECT add_one(k), count(*) FROM t GROUP BY k"),
          Seq(Row(1L, 4L), Row(2L, 3L), Row(3L, 3L)))
        checkAnswer(
          spark.table("t").groupBy("k").agg(addOne(col("k")), count("*")),
          Seq(Row(0L, 1L, 4L), Row(1L, 2L, 3L), Row(2L, 3L, 3L)))
        checkAnswer(sql("SELECT add_one(1), count(*) FROM t"), Row(2L, 10L))
        // The call is the grouping key.
        checkAnswer(
          sql("SELECT add_one(k), count(*) FROM t GROUP BY add_one(k)"),
          Seq(Row(1L, 4L), Row(2L, 3L), Row(3L, 3L)))
        checkAnswer(
          sql("SELECT add_one(k) + 1, sum(add_one(v)) FROM t GROUP BY add_one(k)"),
          Seq(Row(2L, 22L), Row(3L, 15L), Row(4L, 18L)))
      }
    }
  }

  test("calls over aggregate and window functions") {
    withFunctions(addOne, add) {
      withTempView("t") {
        spark.range(10).selectExpr("id % 3 AS k", "id AS v").createOrReplaceTempView("t")
        checkAnswer(sql("SELECT add_one(max(id)) FROM range(10)"), Row(10L))
        checkAnswer(
          sql("SELECT k, add_one(sum(v)) FROM t GROUP BY k"),
          Seq(Row(0L, 19L), Row(1L, 13L), Row(2L, 16L)))
        checkAnswer(
          sql("SELECT k, add(k, sum(add_one(v))) + 1 FROM t GROUP BY k"),
          Seq(Row(0L, 23L), Row(1L, 17L), Row(2L, 21L)))
        checkAnswer(
          spark.table("t").groupBy("k").agg(addOne(sum("v"))),
          Seq(Row(0L, 19L), Row(1L, 13L), Row(2L, 16L)))
        checkAnswer(
          sql("SELECT k, sum(v) FROM t GROUP BY k HAVING add_one(sum(v)) > 13"),
          Seq(Row(0L, 18L), Row(2L, 15L)))
        checkAnswer(
          sql("SELECT DISTINCT add_one(sum(v) OVER (PARTITION BY k)) FROM t"),
          Seq(Row(19L), Row(13L), Row(16L)))
      }
    }
  }

  test("a call in a join condition that reads both sides") {
    val a = spark.range(5).toDF("x")
    val b = spark.range(5).toDF("y")
    val condition = add(a("x"), b("y")) === 1
    checkAnswer(a.join(b, condition), Seq(Row(0L, 1L), Row(1L, 0L)))
    checkAnswer(
      a.join(b, condition && a("x") < b("y")), Seq(Row(0L, 1L)))
    // A call that reads one side stays in the join.
    checkAnswer(
      a.join(b, addOne(a("x")) === b("y"), "left_outer"),
      Seq(Row(0L, 1L), Row(1L, 2L), Row(2L, 3L), Row(3L, 4L), Row(4L, null)))
    checkError(
      exception = intercept[AnalysisException] {
        a.join(b, condition, "left_outer").collect()
      },
      condition = "UNSUPPORTED_FEATURE.COLUMNAR_UDF_IN_JOIN_CONDITION",
      parameters = Map("joinType" -> "LEFT OUTER"))
  }

  test("a call in a correlated subquery that reads both sides") {
    withFunctions(add) {
      withTempView("a", "b") {
        spark.range(5).toDF("x").createOrReplaceTempView("a")
        spark.range(5).toDF("y").createOrReplaceTempView("b")
        val exists = "EXISTS (SELECT 1 FROM b WHERE add(a.x, b.y) = 1)"
        // The subquery becomes a semi join, or an existence join below a disjunction.
        Seq(s"SELECT * FROM a WHERE $exists" -> "LEFT SEMI",
          s"SELECT * FROM a WHERE a.x > 3 OR $exists" -> "EXISTENCE").foreach {
          case (query, joinType) =>
            checkError(
              exception = intercept[AnalysisException](sql(query).collect()),
              condition = "UNSUPPORTED_FEATURE.COLUMNAR_UDF_IN_JOIN_CONDITION",
              parameters = Map("joinType" -> joinType))
        }
        // A call that reads one side is evaluated below the join.
        checkAnswer(
          sql("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE add(b.y, b.y) = a.x)"),
          Seq(Row(0L), Row(2L), Row(4L)))
      }
    }
  }

  test("a call in an equality between the sides of a join is a join key") {
    withFunctions(addOne) {
      withTempView("a", "b") {
        spark.range(5).toDF("x").createOrReplaceTempView("a")
        spark.range(5).toDF("y").createOrReplaceTempView("b")
        val df = sql("SELECT * FROM a, b WHERE add_one(a.x) = b.y")
        checkAnswer(df, (0 until 4).map(i => Row(i.toLong, i + 1L)))
        val joins = df.queryExecution.optimizedPlan.collect { case j: Join => j }
        assert(joins.length == 1 && joins.head.condition.isDefined)
      }
    }
  }

  test("a filter over the result of a call does not evaluate the call again") {
    val df = spark.range(10).select(addOne(col("id")).as("r")).filter(col("r") > 5)
    checkAnswer(df, (6 to 10).map(i => Row(i.toLong)))
    assert(evalNodes(df.queryExecution.executedPlan).length == 1)
  }

  test("a call in a filter over a partition column") {
    withTempPath { path =>
      spark.range(20).selectExpr("id", "id % 4 as p")
        .write.partitionBy("p").parquet(path.getCanonicalPath)
      // With the file source of both DSv1 and DSv2.
      Seq("parquet", "").foreach { useV1SourceList =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceList) {
          val df = spark.read.parquet(path.getCanonicalPath)
            .filter(addOne(col("p")) === 2 && col("id") > 1)
          checkAnswer(df, Seq(5, 9, 13, 17).map(i => Row(i.toLong, 1L)))
          assert(evalNodes(df.queryExecution.executedPlan).length == 1)
        }
      }
    }
  }

  test("a call in the creation side of a runtime bloom filter") {
    withFunctions(addOne) {
      withSQLConf(
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1") {
        withTempPath { path =>
          val big = s"${path.getCanonicalPath}/big"
          val small = s"${path.getCanonicalPath}/small"
          spark.range(1000).selectExpr("id % 100 as k").write.parquet(big)
          spark.range(100).selectExpr("id as k", "id as v").write.parquet(small)
          withTempView("big", "small") {
            spark.read.parquet(big).createOrReplaceTempView("big")
            spark.read.parquet(small).createOrReplaceTempView("small")
            def query(filter: String): DataFrame =
              sql(s"SELECT count(*) FROM big JOIN small ON big.k = small.k WHERE $filter")
            def hasBloomFilter(df: DataFrame): Boolean = df.queryExecution.optimizedPlan.exists {
              _.expressions.exists(_.exists(_.isInstanceOf[BloomFilterMightContain]))
            }
            // The subquery of a bloom filter is planned on its own, after the calls of the
            // query are extracted, so a creation side with a call gets no bloom filter.
            assert(hasBloomFilter(query("small.v + 1 = 5")))
            val df = query("add_one(small.v) = 5")
            assert(!hasBloomFilter(df))
            checkAnswer(df, Row(10L))
          }
        }
      }
    }
  }

  test("a function that is not deterministic in grouping and sort keys") {
    val counter = ColumnarUserDefinedFunction(
      "counter", Seq(LongType), LongType, Some(new CounterFunction), deterministic = false)
    val grouped = spark.range(0, 10, 1, 1).groupBy(counter(col("id"))).count()
    checkAnswer(grouped, (1 to 10).map(i => Row(i.toLong, 1L)))
    val sorted = spark.range(0, 10, 1, 1).orderBy(counter(col("id")).desc)
    assert(sorted.collect().toSeq == (0 until 10).reverse)
  }

  test("a call is a call of a user-defined function") {
    val call = addOne(col("id")).expr
    assert(call.isInstanceOf[UserDefinedExpression])
    // A CHECK constraint or a generated column cannot store it.
    val found = StoredExpressionFunctions.findUserDefinedFunctionCall(
      spark.range(1).select(addOne(col("id"))).queryExecution.analyzed.expressions.head)
    assert(found.map(_._2) == Some("`add_one`"))
    // Single-task execution leaves a query that runs user code alone.
    val optimized = spark.range(1).select(addOne(col("id"))).queryExecution.optimizedPlan
    assert(optimized.containsAnyPattern(MarkSingleTaskExecution.unsupportedPatterns: _*))
  }

  test("plans with equal calls are equal once their expression ids are normalized") {
    def plan: LogicalPlan = spark.range(5).select(addOne(col("id"))).queryExecution.analyzed
    assert(plan != plan)
    assert(NormalizePlan.normalizeExprIds(plan) == NormalizePlan.normalizeExprIds(plan))
  }

  test("a call in a lambda function is not supported") {
    withFunctions(addOne) {
      val e = intercept[AnalysisException] {
        sql("SELECT transform(array(1L, 2L), x -> add_one(x))")
      }
      assert(e.getCondition == "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_COLUMNAR_UDF")
    }
  }

  test("a call with named arguments is not supported") {
    withFunctions(addOne) {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT add_one(x => id) FROM range(3)")
        },
        condition = "NAMED_PARAMETERS_NOT_SUPPORTED",
        parameters = Map("functionName" -> "`add_one`"),
        context = ExpectedContext("add_one(x => id)", 7, 22))
    }
  }

  test("equal calls of a function that is not deterministic are evaluated separately") {
    val counter = ColumnarUserDefinedFunction(
      "counter", Seq(LongType), LongType, Some(new CounterFunction), deterministic = false)
    val df = spark.range(0, 10, 1, 1).select(counter(col("id")), counter(col("id")))
    val rows = df.collect()
    assert(rows.length == 10)
    assert(rows.forall(r => r.getLong(0) != r.getLong(1)))
    val node = evalNodes(df.queryExecution.executedPlan).head
    assert(node.udfs.length == 2)

    // Equal calls of a deterministic function share a result.
    val shared = spark.range(5).select(addOne(col("id")), addOne(col("id")) + 1)
    checkAnswer(shared, (0 until 5).map(i => Row(i + 1L, i + 2L)))
    assert(evalNodes(shared.queryExecution.executedPlan).head.udfs.length == 1)
  }

  test("a call below a limit") {
    val df = spark.range(100000).select(addOne(col("id"))).limit(3)
    assert(df.collect().length == 3)
    val sorted = spark.range(100).select(addOne(col("id")).as("r")).orderBy(col("r").desc).limit(2)
    checkAnswer(sorted, Seq(Row(100L), Row(99L)))
  }

  test("strings and nulls") {
    val df = Seq(Some("a"), None, Some("bc")).toDF("s").select(shout(col("s")))
    checkAnswer(df, Seq(Row("A!"), Row(null), Row("BC!")))
  }

  test("a function that Spark cannot run fails when it is executed") {
    val engineOnly = ColumnarUserDefinedFunction(
      "engine_only", Seq(LongType), LongType,
      engineImplementations = Seq(
        ColumnarUDFImplementation("some-engine", Map("library" -> "/lib.so", "name" -> "f"))))
    val df = spark.range(5).select(engineOnly(col("id")))
    // It is planned, with its implementations on the node, for an engine to replace.
    val node = evalNodes(df.queryExecution.executedPlan).head
    assert(node.udfs.head.engineImplementations.map(_.kind) == Seq("some-engine"))
    checkError(
      exception = intercept[SparkUnsupportedOperationException](df.collect()),
      condition = "COLUMNAR_UDF_NOT_EXECUTABLE",
      parameters = Map("name" -> "engine_only", "kinds" -> "[some-engine]"))
  }

  test("a function that returns one of its arguments fails") {
    val identity = ColumnarUserDefinedFunction(
      "identity", Seq(LongType), LongType, Some(new ReturnArgumentFunction))
    withTempPath { path =>
      spark.range(10).write.parquet(path.getCanonicalPath)
      // Over a columnar scan, and over a row-based child.
      Seq(spark.read.parquet(path.getCanonicalPath), spark.range(10).toDF()).foreach { input =>
        checkError(
          exception = intercept[SparkRuntimeException](input.select(identity(col("id"))).collect()),
          condition = "COLUMNAR_UDF_RESULT_IS_ARGUMENT",
          parameters = Map("name" -> "identity"))
      }
    }
  }

  test("a function that returns a vector of another type fails") {
    val wrongType = ColumnarUserDefinedFunction(
      "wrong_type", Seq(StringType), LongType, Some(new ShoutFunction))
    val df = Seq("a").toDF("s").select(wrongType(col("s")))
    checkError(
      exception = intercept[SparkRuntimeException](df.collect()),
      condition = "COLUMNAR_UDF_RESULT_TYPE_MISMATCH",
      parameters = Map(
        "name" -> "wrong_type", "actualType" -> "\"STRING\"", "expectedType" -> "\"BIGINT\""))
  }
}
