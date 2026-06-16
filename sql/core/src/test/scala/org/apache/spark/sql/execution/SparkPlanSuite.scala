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

package org.apache.spark.sql.execution

import java.lang.management.ManagementFactory
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{SparkEnv, SparkException, SparkUnsupportedOperationException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Alias, Attribute, AttributeReference, Coalesce, CreateArray, Explode, Expression, ExprId,
  Literal, RLike}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Partial}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

class SparkPlanSuite extends SharedSparkSession {

  test("SPARK-21619 execution of a canonicalized plan should fail") {
    val plan = spark.range(10).queryExecution.executedPlan.canonicalized

    intercept[SparkException] { plan.execute() }
    intercept[SparkException] { plan.executeCollect() }
    intercept[SparkException] { plan.executeCollectPublic() }
    intercept[SparkException] { plan.executeToIterator() }
    intercept[SparkException] { plan.executeBroadcast() }
    intercept[SparkException] { plan.executeTake(1) }
    intercept[SparkException] { plan.executeTail(1) }
  }

  test("SPARK-23731 plans should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(1).write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        val fileSourceScanExec =
          df.queryExecution.sparkPlan.collectFirst { case p: FileSourceScanExec => p }.get
        val serializer = SparkEnv.get.serializer.newInstance()
        val readback =
          serializer.deserialize[FileSourceScanExec](serializer.serialize(fileSourceScanExec))
        try {
          readback.canonicalized
        } catch {
          case e: Throwable => fail("FileSourceScanExec was not canonicalizable", e)
        }
      }
    }
  }

  test("SPARK-27418 BatchScanExec should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark.range(1).write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        val batchScanExec =
          df.queryExecution.sparkPlan.collectFirst { case p: BatchScanExec => p }.get
        val serializer = SparkEnv.get.serializer.newInstance()
        val readback =
          serializer.deserialize[BatchScanExec](serializer.serialize(batchScanExec))
        try {
          readback.canonicalized
        } catch {
          case e: Throwable => fail("BatchScanExec was not canonicalizable", e)
        }
      }
    }
  }

  test("SPARK-25357 SparkPlanInfo of FileScan contains nonEmpty metadata") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(5).write.parquet(path.getAbsolutePath)
        val f = spark.read.parquet(path.getAbsolutePath)
        assert(SparkPlanInfo.fromSparkPlan(f.queryExecution.sparkPlan).metadata.nonEmpty)
      }
    }
  }

  test("SPARK-30780 empty LocalTableScan should use RDD without partitions") {
    assert(LocalTableScanExec(Nil, Nil, None).execute().getNumPartitions == 0)
  }

  test("SPARK-33617: change default parallelism of LocalTableScan") {
    Seq(1, 4).foreach { minPartitionNum =>
      withSQLConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> minPartitionNum.toString) {
        val df = spark.sql("SELECT * FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8)")
        assert(df.rdd.partitions.length === minPartitionNum)
      }
    }
  }

  test("SPARK-34420: Throw exception if non-streaming Deduplicate is not replaced by aggregate") {
    val df = spark.range(10)
    val planner = spark.sessionState.planner
    val deduplicate = Deduplicate(df.queryExecution.analyzed.output, df.queryExecution.analyzed)
    checkError(
      exception = intercept[SparkException] {
        planner.plan(deduplicate)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> ("Deduplicate operator for non streaming data source should have been " +
          "replaced by aggregate in the optimizer")))
  }

  test("SPARK-37221: The collect-like API in SparkPlan should support columnar output") {
    val emptyResults = ColumnarOp(LocalTableScanExec(Nil, Nil, None)).toRowBased.executeCollect()
    assert(emptyResults.isEmpty)

    val relation = LocalTableScanExec(
      Seq(AttributeReference("val", IntegerType)()), Seq(InternalRow(1)), None)
    val nonEmpty = ColumnarOp(relation).toRowBased.executeCollect()
    assert(nonEmpty === relation.executeCollect())
  }

  test("ColumnarToRowExec should materialize null values from non-nullable columnar output") {
    val output = Seq(AttributeReference("value", StringType, nullable = false)())

    def assertSingleNull(rows: Array[InternalRow]): Unit = {
      assert(rows.length === 1)
      assert(rows.head.isNullAt(0))
    }

    def assertAllNull(rows: Array[InternalRow]): Unit = {
      assert(rows.length === 1)
      assert(rows.head.isNullAt(0))
      assert(rows.head.isNullAt(1))
    }

    def columnarLeaf(): NullColumnarOp = NullColumnarOp(output)

    def columnarToRow(): ColumnarToRowExec = ColumnarToRowExec(columnarLeaf())

    def withTransitions(plan: SparkPlan): SparkPlan = {
      ApplyColumnarRulesAndInsertTransitions(Nil, outputsColumnar = false).apply(plan)
    }

    def projected(): ProjectExec = {
      withTransitions(ProjectExec(output, columnarLeaf())).asInstanceOf[ProjectExec]
    }

    def splitProject(child: SparkPlan): WholeStageCodegenExec = {
      val projectList = Seq(
        Alias(child.output.head, "first")(),
        Alias(child.output.head, "second")())
      WholeStageCodegenExec(ProjectExec(projectList, child))(codegenStageId = 0)
    }

    Seq(
      WholeStageCodegenExec(projected())(codegenStageId = 0).executeCollect(),
      WholeStageCodegenExec(columnarToRow())(codegenStageId = 0).executeCollect(),
      columnarToRow().executeCollect()).foreach(assertSingleNull)

    withSQLConf(SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> "true") {
      val split = splitProject(projected())
      assert(split.doCodeGen()._2.body.contains("project_doConsume"))
      assertAllNull(split.executeCollect())
    }

    val rowBoundaryProjected = projected()
    Seq(
      WholeStageCodegenExec(
        withTransitions(
          FilterExec(RLike(output.head, Literal("present")), ProjectExec(output, columnarLeaf()))))(
          codegenStageId = 0).executeCollect(),
      WholeStageCodegenExec(
        FilterExec(
          RLike(rowBoundaryProjected.output.head, Literal("present")),
          InputAdapter(rowBoundaryProjected)))(codegenStageId = 0).executeCollect())
      .foreach { rows =>
        assert(rows.isEmpty)
      }

    val splitProjected = projected()
    assert(splitProjected.output.head.nullable)
    val rowBoundary = InputAdapter(splitProjected)
    withSQLConf(SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> "true") {
      val splitRegex = splitProject(FilterExec(
        Coalesce(Seq(RLike(rowBoundary.output.head, Literal("present")), Literal(true))),
        rowBoundary))
      assert(splitRegex.doCodeGen()._2.body.contains("project_doConsume"))
      assertAllNull(splitRegex.executeCollect())
    }

    def partialGroupedAggregate(): HashAggregateExec = {
      val partialCount = Count(Literal(1)).toAggregateExpression().copy(mode = Partial)
      withTransitions(HashAggregateExec(
        requiredChildDistributionExpressions = None,
        isStreaming = false,
        numShufflePartitions = None,
        groupingExpressions = output,
        aggregateExpressions = Seq(partialCount),
        aggregateAttributes = partialCount.aggregateFunction.aggBufferAttributes,
        initialInputBufferOffset = 0,
        resultExpressions =
          output.map(_.toAttribute) ++ partialCount.aggregateFunction.inputAggBufferAttributes,
        child = columnarLeaf()))
        .asInstanceOf[HashAggregateExec]
    }

    def assertPartialAggregateNull(rows: Array[InternalRow]): Unit = {
      assert(rows.length === 1)
      assert(rows.head.isNullAt(0))
      assert(rows.head.getLong(1) === 1L)
    }

    assert(partialGroupedAggregate().canonicalized != null)
    assert(partialGroupedAggregate().output.head.nullable)
    Seq(
      WholeStageCodegenExec(partialGroupedAggregate())(codegenStageId = 0).executeCollect(),
      partialGroupedAggregate().executeCollect(),
      partialGroupedAggregate().toSortAggregate.executeCollect()).foreach(assertPartialAggregateNull)

    def generate(): GenerateExec = {
      withTransitions(GenerateExec(
        Explode(CreateArray(Seq(Literal(1)))),
        requiredChildOutput = output,
        outer = false,
        generatorOutput = Seq(AttributeReference("col", IntegerType, nullable = false)()),
        child = columnarLeaf()))
        .asInstanceOf[GenerateExec]
    }

    def assertGeneratedNull(rows: Array[InternalRow]): Unit = {
      assert(rows.length === 1)
      assert(rows.head.isNullAt(0))
      assert(rows.head.getInt(1) === 1)
    }

    assert(generate().output.head.nullable)
    Seq(
      WholeStageCodegenExec(generate())(codegenStageId = 0).executeCollect(),
      generate().executeCollect()).foreach(assertGeneratedNull)

    def takeOrderedAndProject(): TakeOrderedAndProjectExec = {
      withTransitions(TakeOrderedAndProjectExec(
        limit = 1,
        sortOrder = Nil,
        projectList = output,
        child = columnarLeaf()))
        .asInstanceOf[TakeOrderedAndProjectExec]
    }

    assert(takeOrderedAndProject().output.head.nullable)
    assertSingleNull(takeOrderedAndProject().executeCollect())

    def expanded(): ExpandExec = {
      withTransitions(ExpandExec(
        projections = Seq(output),
        output = output,
        child = columnarLeaf()))
        .asInstanceOf[ExpandExec]
    }

    assert(expanded().output.head.nullable)
    Seq(
      WholeStageCodegenExec(expanded())(codegenStageId = 0).executeCollect(),
      expanded().executeCollect()).foreach(assertSingleNull)
  }

  test("BatchScanExec hashCode includes keyGroupedPartitioning") {
    // hashCode must include all fields used in equals.
    // Previously keyGroupedPartitioning was in equals but missing from hashCode,
    // violating the contract that equal objects must have equal hash codes.
    val exec1 = BatchScanExec(
      output = Seq.empty,
      scan = null,
      runtimeFilters = Seq.empty,
      table = null,
      keyGroupedPartitioning = Some(Seq(Literal(1)))
    )
    val exec2 = BatchScanExec(
      output = Seq.empty,
      scan = null,
      runtimeFilters = Seq.empty,
      table = null,
      keyGroupedPartitioning = Some(Seq(Literal(2)))
    )
    // With null batch, equals returns false (by design, see SPARK-42745),
    // so we only verify that hashCode differs for different keyGroupedPartitioning.
    assert(exec1.hashCode() != exec2.hashCode())
  }

  test("SPARK-37779: ColumnarToRowExec should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(1).write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        val columnarToRowExec =
          df.queryExecution.executedPlan.collectFirst { case p: ColumnarToRowExec => p }.get
        try {
          spark.range(1).foreach { _ =>
            columnarToRowExec.canonicalized
            ()
          }
        } catch {
          case e: Throwable => fail("ColumnarToRowExec was not canonicalizable", e)
        }
      }
    }
  }

  test("SPARK-57041: waitForSubqueries must not hold the plan's monitor " +
    "while awaiting subquery results") {
    val enteredLatch = new CountDownLatch(1)
    val releaseLatch = new CountDownLatch(1)

    val subqueryExec = TestSubqueryExec(LocalTableScanExec(Nil, Nil, None))
    val subqueryExpr = BlockingSubquery(subqueryExec, ExprId(0), enteredLatch, releaseLatch)
    val plan = TestPlanWithSubquery(subqueryExpr)

    val executor = ThreadUtils.newDaemonSingleThreadExecutor("test-wait-for-subqueries")
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    plan.testPrepare()
    val futureA = Future { plan.testWaitForSubqueries() }

    try {
      assert(enteredLatch.await(10, TimeUnit.SECONDS),
        "Thread A did not enter updateResult() within 10s")

      val threadB = new Thread(() => plan.synchronized {})
      threadB.setDaemon(true)
      threadB.start()

      val bean = ManagementFactory.getThreadMXBean
      val deadline = System.currentTimeMillis() + 5000L
      var threadBBlocked = false
      var waiting = true
      while (waiting) {
        if (!threadB.isAlive || System.currentTimeMillis() > deadline) {
          waiting = false
        } else {
          val state = Option(bean.getThreadInfo(threadB.getId)).map(_.getThreadState).orNull
          if (state == Thread.State.BLOCKED) {
            threadBBlocked = true
            waiting = false
          } else if (state != null) {
            Thread.sleep(1)
          }
        }
      }

      releaseLatch.countDown()
      ThreadUtils.awaitResult(futureA, Duration(10, "seconds"))
      threadB.join(5000L)

      assert(!threadBBlocked,
        "Deadlock: plan.this.synchronized could not be acquired while waitForSubqueries() was " +
        "blocking on a subquery future. waitForSubqueries() must not hold the plan's monitor.")
    } finally {
      releaseLatch.countDown()
      executor.shutdown()
    }
  }
}

case class ColumnarOp(child: SparkPlan) extends UnaryExecNode {
  override val supportsColumnar: Boolean = true
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    RowToColumnarExec(child).executeColumnar()
  override protected def doExecute(): RDD[InternalRow] = throw SparkUnsupportedOperationException()
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarOp =
    copy(child = newChild)
}

case class NullColumnarOp(override val output: Seq[Attribute])
  extends LeafExecNode with CodegenSupport {
  override val supportsColumnar: Boolean = true
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    sparkContext.parallelize(Seq(0), 1).map { _ =>
      val column = new OnHeapColumnVector(1, StringType)
      column.putNull(0)
      new ColumnarBatch(Array(column), 1)
    }
  }
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def inputRDDs(): Seq[RDD[InternalRow]] = throw new UnsupportedOperationException()
  override protected def doProduce(ctx: CodegenContext): String =
    throw new UnsupportedOperationException()
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    throw new UnsupportedOperationException()
}

private case class TestSubqueryExec(child: SparkPlan) extends BaseSubqueryExec {
  override def name: String = "TestSubqueryExec"
  override def children: Seq[SparkPlan] = Seq(child)
  override protected def doExecute(): RDD[InternalRow] = child.execute()
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): TestSubqueryExec = copy(child = newChildren.head)
}

private case class BlockingSubquery(
    plan: BaseSubqueryExec,
    exprId: ExprId,
    enteredLatch: CountDownLatch,
    releaseLatch: CountDownLatch)
    extends ExecSubqueryExpression with LeafLike[Expression] {

  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = null
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException("test only")
  override def withNewPlan(plan: BaseSubqueryExec): ExecSubqueryExpression =
    copy(plan = plan)

  override def updateResult(): Unit = {
    enteredLatch.countDown()
    releaseLatch.await(30, TimeUnit.SECONDS)
  }
}

private case class TestPlanWithSubquery(subqueryExpr: ExecSubqueryExpression)
    extends LeafExecNode {
  override def output: Seq[Attribute] = Nil
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("test only")
  def testPrepare(): Unit = prepare()
  def testWaitForSubqueries(): Unit = waitForSubqueries()
}
