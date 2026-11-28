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

package org.apache.spark.sql.execution.python

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EvalOnlyWhen, Expression, If, Literal, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, ArrowEvalPython, BatchEvalPython, Limit, LocalLimit, LocalRelation, Project}
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType, StringType, StructType}

class ExtractPythonUDFsSuite extends SharedSparkSession {
  import testImplicits._

  val batchedPythonUDF = new MyDummyPythonUDF
  val batchedNondeterministicPythonUDF = new MyDummyNondeterministicPythonUDF
  val scalarPandasUDF = new MyDummyScalarPandasUDF

  private def collectBatchExec(plan: SparkPlan): Seq[BatchEvalPythonExec] = plan.collect {
    case b: BatchEvalPythonExec => b
  }

  private def collectArrowExec(plan: SparkPlan): Seq[ArrowEvalPythonExec] = plan.collect {
    case b: ArrowEvalPythonExec => b
  }

  test("SPARK-61482: a UDF inside an EvalOnlyWhen marker gets the condition, " +
      "others do not, and they are planned in separate nodes") {
    val a = AttributeReference("a", LongType)()
    val b = AttributeReference("b", LongType)()
    val flag = AttributeReference("flag", BooleanType)()
    val rel = LocalRelation(a, b, flag)
    val marked = batchedPythonUDF.builder(Seq(b))
    val plain = batchedPythonUDF.builder(Seq(a))
    // flag ? EvalOnlyWhen(flag, dummy(b)) : null  (marked), and dummy(a) unmarked.
    val proj = Project(
      Seq(
        Alias(plain, "p")(),
        Alias(If(flag, EvalOnlyWhen(flag, marked), Literal.create(null, BooleanType)), "m")()),
      rel)
    val extracted = ExtractPythonUDFs(proj)
    val conds = extracted.collect { case e: BatchEvalPython => e.evalCondition }
    // Two eval nodes (different conditions), one with the marker's condition, one without.
    assert(conds.length == 2)
    assert(conds.count(_.isEmpty) == 1)
    assert(conds.count(_.contains(flag)) == 1)
  }

  test("SPARK-61482: the eval condition reaches the physical node and disables columnar input") {
    val a = AttributeReference("a", LongType)()
    val flag = AttributeReference("flag", BooleanType)()
    val rel = LocalRelation(a, flag)
    val udf = batchedPythonUDF.builder(Seq(a)).asInstanceOf[PythonUDF]
    val resultAttr = AttributeReference("pythonUDF0", udf.dataType)()
    // A BatchEvalPython with a condition plans to a BatchEvalPythonExec that keeps it.
    val batch = BatchEvalPython(Seq(udf), rel.output, Seq(resultAttr), rel, Some(flag))
    val batchExec = spark.sessionState.planner.plan(batch).next()
    val batchConds = batchExec.collect { case b: BatchEvalPythonExec => b.evalCondition }.flatten
    assert(batchConds == Seq(flag))
    // An ArrowEvalPython with a condition does not take columnar input. Use a columnar-capable
    // child so that `supportsColumnar` turns on `evalCondition.isEmpty` rather than on the
    // child (a LocalRelation plans to a row-based scan).
    val columnarChild = ColumnarLeafExec(Seq(a))
    def arrowExec(cond: Option[Expression]): ArrowEvalPythonExec =
      ArrowEvalPythonExec(Seq(udf), Seq(a), Seq(resultAttr), columnarChild,
        PythonEvalType.SQL_ARROW_BATCHED_UDF, cond)(columnarInputEnabled = true)
    assert(arrowExec(None).supportsColumnar)
    assert(!arrowExec(Some(flag)).supportsColumnar)
  }

  test("Chained Batched Python UDFs should be combined to a single physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", batchedPythonUDF(col("a")))
      .withColumn("d", batchedPythonUDF(col("c")))
    val pythonEvalNodes = collectBatchExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
  }

  test("Chained Scalar Pandas UDFs should be combined to a single physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", scalarPandasUDF(col("a")))
      .withColumn("d", scalarPandasUDF(col("c")))
    val arrowEvalNodes = collectArrowExec(df2.queryExecution.executedPlan)
    assert(arrowEvalNodes.size == 1)
  }

  test("Mixed Batched Python UDFs and Pandas UDF should be separate physical node") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c", batchedPythonUDF(col("a")))
      .withColumn("d", scalarPandasUDF(col("b")))

    val pythonEvalNodes = collectBatchExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectArrowExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
    assert(arrowEvalNodes.size == 1)
  }

  test("Independent Batched Python UDFs and Scalar Pandas UDFs should be combined separately") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c1", batchedPythonUDF(col("a")))
      .withColumn("c2", batchedPythonUDF(col("c1")))
      .withColumn("d1", scalarPandasUDF(col("a")))
      .withColumn("d2", scalarPandasUDF(col("d1")))

    val pythonEvalNodes = collectBatchExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectArrowExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
    assert(arrowEvalNodes.size == 1)
  }

  test("Dependent Batched Python UDFs and Scalar Pandas UDFs should not be combined") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = df.withColumn("c1", batchedPythonUDF(col("a")))
      .withColumn("d1", scalarPandasUDF(col("c1")))
      .withColumn("c2", batchedPythonUDF(col("d1")))
      .withColumn("d2", scalarPandasUDF(col("c2")))

    val pythonEvalNodes = collectBatchExec(df2.queryExecution.executedPlan)
    val arrowEvalNodes = collectArrowExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 2)
    assert(arrowEvalNodes.size == 2)
  }

  test("Python UDF should not break column pruning/filter pushdown -- Parquet V1") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { f =>
        spark.range(10).select($"id".as("a"), $"id".as("b"))
          .write.parquet(f.getCanonicalPath)
        val df = spark.read.parquet(f.getCanonicalPath)

        withClue("column pruning") {
          val query = df.filter(batchedPythonUDF($"a")).select($"a")

          val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
          assert(pythonEvalNodes.length == 1)

          val scanNodes = query.queryExecution.executedPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scanNodes.length == 1)
          assert(scanNodes.head.output.map(_.name) == Seq("a"))
        }

        withClue("filter pushdown") {
          val query = df.filter($"a" > 1 && batchedPythonUDF($"a"))
          val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
          assert(pythonEvalNodes.length == 1)

          val scanNodes = query.queryExecution.executedPlan.collect {
            case scan: FileSourceScanExec => scan
          }
          assert(scanNodes.length == 1)
          // $"a" is not null and $"a" > 1
          assert(scanNodes.head.dataFilters.length == 2)
          assert(scanNodes.head.dataFilters.flatMap(_.references.map(_.name)).distinct == Seq("a"))
        }
      }
    }
  }

  test("Python UDF should not break column pruning/filter pushdown -- Parquet V2") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { f =>
        spark.range(10).select($"id".as("a"), $"id".as("b"))
          .write.parquet(f.getCanonicalPath)
        val df = spark.read.parquet(f.getCanonicalPath)

        withClue("column pruning") {
          val query = df.filter(batchedPythonUDF($"a")).select($"a")

          val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
          assert(pythonEvalNodes.length == 1)

          val scanNodes = query.queryExecution.executedPlan.collect {
            case scan: BatchScanExec => scan
          }
          assert(scanNodes.length == 1)
          assert(scanNodes.head.output.map(_.name) == Seq("a"))
        }

        withClue("filter pushdown") {
          val query = df.filter($"a" > 1 && batchedPythonUDF($"a"))
          val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
          assert(pythonEvalNodes.length == 1)

          val scanNodes = query.queryExecution.executedPlan.collect {
            case scan: BatchScanExec => scan
          }
          assert(scanNodes.length == 1)
          // $"a" is not null and $"a" > 1
          val filters = scanNodes.head.scan.asInstanceOf[ParquetScan].pushedFilters
          assert(filters.length == 2)
          assert(filters.flatMap(_.references).distinct === Array("a"))
        }
      }
    }
  }

  test("SPARK-61614: A filter on the result of a Python UDF over a scan does not evaluate it " +
      "again") {
    withClue("V2 scan") {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        withTempPath { f =>
          spark.range(10).select($"id".as("a"), $"id".as("b")).write.parquet(f.getCanonicalPath)
          val query = spark.read.parquet(f.getCanonicalPath)
            .select(batchedPythonUDF($"a").as("u"), $"b").where($"u")
          assert(collectBatchExec(query.queryExecution.executedPlan).length == 1)
        }
      }
    }
    withClue("V1 partition pruning") {
      withTable("t") {
        spark.range(10).select($"id".as("a"), ($"id" % 2).as("p"))
          .write.partitionBy("p").saveAsTable("t")
        val query = spark.table("t")
          .select(batchedPythonUDF($"a").as("u"), $"p").where($"u" && $"p" === 1)
        assert(collectBatchExec(query.queryExecution.executedPlan).length == 1)
      }
    }
  }

  test("SPARK-33303: Deterministic UDF calls are deduplicated") {
    val df = Seq("Hello").toDF("a")

    val df2 = df.withColumn("c", batchedPythonUDF(col("a"))).withColumn("d", col("c"))
    val pythonEvalNodes2 = collectBatchExec(df2.queryExecution.executedPlan)
    assert(pythonEvalNodes2.size == 1)
    assert(pythonEvalNodes2.head.udfs.size == 1)

    val df3 = df.withColumns(Seq("c", "d"),
      Seq(batchedPythonUDF(col("a")), batchedPythonUDF(col("a"))))
    val pythonEvalNodes3 = collectBatchExec(df3.queryExecution.executedPlan)
    assert(pythonEvalNodes3.size == 1)
    assert(pythonEvalNodes3.head.udfs.size == 1)

    val df4 = df.withColumn("c", batchedNondeterministicPythonUDF(col("a")))
      .withColumn("d", col("c"))
    val pythonEvalNodes4 = collectBatchExec(df4.queryExecution.executedPlan)
    assert(pythonEvalNodes4.size == 1)
    assert(pythonEvalNodes4.head.udfs.size == 1)

    val df5 = df.withColumns(Seq("c", "d"),
      Seq(batchedNondeterministicPythonUDF(col("a")), batchedNondeterministicPythonUDF(col("a"))))
    val pythonEvalNodes5 = collectBatchExec(df5.queryExecution.executedPlan)
    assert(pythonEvalNodes5.size == 1)
    assert(pythonEvalNodes5.head.udfs.size == 2)
  }

  test("SPARK-62106: A nondeterministic UDF call that is selected twice is evaluated once") {
    val call = batchedNondeterministicPythonUDF(col("a"))
    val df = Seq("Hello").toDF("a").select(call.as("c"), call.as("d"))
    val pythonEvalNodes = collectBatchExec(df.queryExecution.executedPlan)
    assert(pythonEvalNodes.size == 1)
    assert(pythonEvalNodes.head.udfs.size == 1)
  }

  test("SPARK-62106: Equal UDF calls over an aggregate function are evaluated once") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    // The numbers of aggregate expressions and of evaluated UDF calls in the optimized plan.
    def sizes(agg: () => Column): Seq[Int] = {
      val plan = df.agg(batchedPythonUDF(agg()).as("c"), batchedPythonUDF(agg()).as("d"))
        .queryExecution.optimizedPlan
      plan.collect { case a: Aggregate => a.aggregateExpressions.size } ++
        plan.collect { case b: BatchEvalPython => b.udfs.size }
    }
    assert(sizes(() => sum(col("b"))) == Seq(1, 1))
    // Calls of a nondeterministic aggregate function are kept apart.
    val nondeterministic = new MyDummyGroupedAggPandasUDF().copy(udfDeterministic = false)
    assert(sizes(() => nondeterministic(col("b"))) == Seq(2, 2))
  }

  test("Infers LocalLimit for Python evaluator") {
    val df = Seq(("Hello", 4), ("World", 8)).toDF("a", "b")

    // Check that PushProjectionThroughLimitAndOffset brings GlobalLimit - LocalLimit to the top
    // (for CollectLimit) and that LimitPushDown keeps LocalLimit under UDF.
    val df2 = df.limit(1).select(batchedPythonUDF(col("b")))
    assert(df2.queryExecution.optimizedPlan match {
      case Limit(_, _) => true
    })
    assert(df2.queryExecution.optimizedPlan.find {
      case b: BatchEvalPython => b.child.isInstanceOf[LocalLimit]
      case _ => false
    }.isDefined)

    val df3 = df.limit(1).select(scalarPandasUDF(col("b")))
    assert(df3.queryExecution.optimizedPlan match {
      case Limit(_, _) => true
    })
    assert(df3.queryExecution.optimizedPlan.find {
      case a: ArrowEvalPython => a.child.isInstanceOf[LocalLimit]
      case _ => false
    }.isDefined)

    val df4 = df.limit(1).select(batchedPythonUDF(col("b")), scalarPandasUDF(col("b")))
    assert(df4.queryExecution.optimizedPlan match {
      case Limit(_, _) => true
    })
    val evalsWithLimit = df4.queryExecution.optimizedPlan.collect {
      case b: BatchEvalPython if b.child.isInstanceOf[LocalLimit] => b
      case a: ArrowEvalPython if a.child.isInstanceOf[LocalLimit] => a
    }
    assert(evalsWithLimit.length == 2)

    // Check that LimitPushDown properly pushes LocalLimit past EvalPython operators.
    val df5 = df.select(batchedPythonUDF(col("b")), scalarPandasUDF(col("b"))).limit(1)
    df5.queryExecution.optimizedPlan.foreach {
      case b: BatchEvalPython => assert(b.child.isInstanceOf[LocalLimit])
      case a: ArrowEvalPython => assert(a.child.isInstanceOf[LocalLimit])
      case _ =>
    }
  }

  // A relation with the columns a, b and c, which are not aliases of other columns.
  private def threeColumns: DataFrame = spark.createDataFrame(
    java.util.List.of(Row("Hello", 4L, 1.0)),
    new StructType().add("a", StringType).add("b", LongType).add("c", DoubleType))

  test("Python UDF evaluation passes through only the columns of its child that are used") {
    val df = threeColumns
    val arrowIterUDF = new MyDummyScalarArrowIterUDF
    // The physical nodes evaluating the UDFs, with the names of the columns they pass through.
    // Adaptive execution is disabled to see the plan below an exchange before it runs.
    def passThrough(df: DataFrame): Seq[(String, Seq[String])] =
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        df.queryExecution.executedPlan.collect {
          case e: EvalPythonExec =>
            assert(e.output == e.requiredChildOutput ++ e.resultAttrs)
            assert(e.requiredChildOutput.forall(e.child.outputSet.contains))
            (e.nodeName, e.requiredChildOutput.map(_.name))
        }
      }

    Seq(
      ("BatchEvalPython", batchedPythonUDF),
      ("ArrowEvalPython", scalarPandasUDF),
      ("ArrowEvalPython", arrowIterUDF)).foreach { case (nodeName, udf) =>
      withClue(s"$nodeName: ") {
        // The arguments are not passed through unless the parent uses them.
        assert(passThrough(df.select(udf(col("a")))) == Seq(nodeName -> Nil))
        assert(passThrough(df.select(col("b"), udf(col("a")))) == Seq(nodeName -> Seq("b")))
        assert(passThrough(df.select(col("a"), udf(col("a")))) == Seq(nodeName -> Seq("a")))
        assert(passThrough(df.withColumn("u", udf(col("a")))) ==
          Seq(nodeName -> Seq("a", "b", "c")))
        // Through a filter on the result, and a filter on the result alone.
        assert(passThrough(df.select(udf(col("a")).as("u"), col("c")).where(col("u"))) ==
          Seq(nodeName -> Seq("c")))
        assert(passThrough(df.where(udf(col("a"))).select(col("b"))) ==
          Seq(nodeName -> Seq("b")))
        // Under an aggregate.
        assert(passThrough(df.groupBy(col("b")).agg(count(udf(col("a"))))) ==
          Seq(nodeName -> Seq("b")))
      }
    }

    // Stacked evaluations of different eval types: the lower one passes through only the
    // argument of the upper one, its result.
    val stacked = df.select(scalarPandasUDF(batchedPythonUDF(col("a"))))
    assert(passThrough(stacked) == Seq("ArrowEvalPython" -> Nil, "BatchEvalPython" -> Nil))
    val stacked2 = df.select(col("c"), scalarPandasUDF(batchedPythonUDF(col("a"))))
    assert(passThrough(stacked2) ==
      Seq("ArrowEvalPython" -> Seq("c"), "BatchEvalPython" -> Seq("c")))
  }

  test("The logical Python UDF evaluation passes through only the columns used by its parent") {
    val df = threeColumns
    val evals = df.select(col("b"), scalarPandasUDF(col("a")))
      .queryExecution.optimizedPlan.collect { case e: ArrowEvalPython => e }
    assert(evals.length == 1)
    assert(evals.head.requiredChildOutput.map(_.name) == Seq("b"))
    assert(evals.head.output == evals.head.requiredChildOutput ++ evals.head.resultAttrs)
    // The child is pruned to the columns passed through and the arguments.
    assert(evals.head.child.output.map(_.name).toSet == Set("a", "b"))
  }
}

/** A leaf whose output supports columnar input; it is never executed. */
private case class ColumnarLeafExec(output: Seq[Attribute]) extends LeafExecNode {
  override def supportsColumnar: Boolean = true
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException()
}
