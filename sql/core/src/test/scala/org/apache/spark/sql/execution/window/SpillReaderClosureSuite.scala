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

package org.apache.spark.sql.execution.window

import java.lang.management.ManagementFactory

import com.sun.management.UnixOperatingSystemMXBean

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.config.UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, Min, Sum}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{CollapseCodegenStages, ExternalAppendOnlyUnsafeRowArray, SparkPlan, SQLExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class SpillReaderClosureSuite extends QueryTest with SharedSparkSession {
  import testImplicits._
  import SpillReaderClosureSuite.openFileCount

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED, false)

  Seq(LeftSemi, LeftAnti, ExistenceJoin(AttributeReference("exists", BooleanType)())).foreach {
    joinType =>
      testWithWholeStageCodegenOnAndOff(s"SPARK-61251: $joinType closes an early scan") {
        codegenEnabled =>
          withSQLConf(
              SQLConf.SHUFFLE_PARTITIONS.key -> "1",
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
              SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
              SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "10") {
            val left = spark.sparkContext.parallelize(
              (0 until 16).map(v => (0, v)) :+ (0, 100), 1).toDF("lk", "lv")
            val right = spark.sparkContext.parallelize(
              (0 until 50).map(v => (0, v)), 1).toDF("rk", "rv")
            val join = SortMergeJoinExec(
              Seq(left("lk").expr), Seq(right("rk").expr), joinType,
              Some(GreaterThan(right("rv").expr, left("lv").expr)),
              left.queryExecution.sparkPlan, right.queryExecution.sparkPlan)
            val plan = CollapseCodegenStages().apply(new EnsureRequirements().apply(join))
            assert(plan.isInstanceOf[WholeStageCodegenExec] == codegenEnabled.toBoolean)
            val expected = joinType match {
              case LeftSemi => (0 until 16).map(v => Row(0, v))
              case LeftAnti => Seq(Row(0, 100))
              case _ => (0 until 16).map(v => Row(0, v, true)) :+ Row(0, 100, false)
            }
            checkFilesAndRows(plan, expected)
          }
      }
  }

  Seq(LeftOuter, LeftAnti, ExistenceJoin(AttributeReference("exists", BooleanType)())).foreach {
    joinType =>
      testWithWholeStageCodegenOnAndOff(s"SPARK-61251: $joinType skips a guarded scan") { _ =>
        withSQLConf(
            SQLConf.SHUFFLE_PARTITIONS.key -> "1",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
            SQLConf.SPLIT_STREAMED_SIDE_JOIN_CONDITION.key -> "true",
            SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
            SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "10") {
          val left = spark.sparkContext.parallelize(
            (0 until 16).map(v => (0, v)), 1).toDF("lk", "lv")
          val right = spark.sparkContext.parallelize(
            (0 until 50).map(v => (0, v)), 1).toDF("rk", "rv")
          val condition = And(LessThan(left("lv").expr, Literal(0)),
            GreaterThan(right("rv").expr, left("lv").expr))
          val join = SortMergeJoinExec(
            Seq(left("lk").expr), Seq(right("rk").expr), joinType, Some(condition),
            left.queryExecution.sparkPlan, right.queryExecution.sparkPlan)
          val plan = CollapseCodegenStages().apply(new EnsureRequirements().apply(join))
          val expected = joinType match {
            case LeftOuter => (0 until 16).map(v => Row(0, v, null, null))
            case LeftAnti => (0 until 16).map(v => Row(0, v))
            case _ => (0 until 16).map(v => Row(0, v, false))
          }
          checkFilesAndRows(plan, expected)
        }
      }
  }

  testWithWholeStageCodegenOnAndOff("SPARK-61251: following window closes duplicate-key scans") {
    _ =>
      withSQLConf(
          SQLConf.SHUFFLE_PARTITIONS.key -> "1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false",
          SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
          SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "10") {
        withTempView("spill_window") {
          spark.range(0, 40, 1, 1).selectExpr("0 AS k", "id AS v")
            .createOrReplaceTempView("spill_window")
          val df = sql("SELECT v, sum(v) OVER (ORDER BY k " +
            "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM spill_window")
          checkFilesAndRows(df.queryExecution.executedPlan,
            (0L until 40L).map(v => Row(v, 780L)))
        }
      }
  }

  testWithWholeStageCodegenOnAndOff("SPARK-61251: segment tree closes partial-block scans") { _ =>
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1",
        SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "16",
        SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
        SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "64") {
      withTempView("spill_window") {
        spark.range(0, 256, 1, 1).createOrReplaceTempView("spill_window")
        val df = sql("SELECT id, sum(id) OVER (ORDER BY id " +
          "ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM spill_window")
        // Keep the two persistent cursors small while exercising many partial blocks.
        checkFilesAndRows(df.queryExecution.executedPlan,
          (0L until 256L).map(v => Row(v, if (v == 255) v else v + v + 1)),
          segmentTree = true)
      }
    }
  }

  Seq(LeftSemi, LeftAnti, ExistenceJoin(AttributeReference("exists", BooleanType)())).foreach {
    joinType =>
      testWithWholeStageCodegenOnAndOff(s"SPARK-61251: $joinType closes failed scans") { _ =>
        assume(openFileCount().isDefined, "needs a JVM that counts its open files")
        withSQLConf(
            SQLConf.SHUFFLE_PARTITIONS.key -> "1",
            SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
            SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "10") {
          val left = spark.sparkContext.parallelize(
            (0 until 17).map(v => (0, v)), 1).toDF("lk", "lv")
          val right = spark.sparkContext.parallelize(
            (0 until 50).map(v => (0, v)), 1).toDF("rk", "rv")
          val failure = RaiseError(Literal("expected failure")).copy(dataType = BooleanType)
          val join = SortMergeJoinExec(
            Seq(left("lk").expr), Seq(right("rk").expr), joinType, Some(failure),
            left.queryExecution.sparkPlan, right.queryExecution.sparkPlan)
          val plan = CollapseCodegenStages().apply(new EnsureRequirements().apply(join))
          val observed = SQLExecution.withSQLConfPropagated(spark) {
            plan.execute().mapPartitions { iter =>
              Iterator.single(SpillReaderClosureSuite.failureGrowth(() => {
                if (iter.hasNext) iter.next()
              }))
            }.collect()
          }
          assert(observed.length == 1)
          assert(observed.head._1 == 16)
          info(s"Additional open files after caught failures: ${observed.head._2}")
          assert(observed.head._2 < 8)
        }
      }
  }

  Seq(false, true).foreach { segmentTree =>
    testWithWholeStageCodegenOnAndOff(
      s"SPARK-61251: window closes failed scans (segmentTree=$segmentTree)") { codegen =>
      assume(openFileCount().isDefined, "needs a JVM that counts its open files")
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key ->
          (if (codegen.toBoolean) "CODEGEN_ONLY" else "NO_CODEGEN")) {
        val observed = SQLExecution.withSQLConfPropagated(spark) {
          spark.sparkContext.parallelize(Seq(0), 1).mapPartitions { _ =>
            val attr = AttributeReference("v", IntegerType)()
            val projection = UnsafeProjection.create(Seq(attr), Seq(attr))
            val rows = new ExternalAppendOnlyUnsafeRowArray(1, Long.MaxValue, 10, Long.MaxValue)
            (0 until 50).foreach(v => rows.add(projection(InternalRow(v))))
            val failure = RaiseError(Literal("expected failure")).copy(dataType = IntegerType)
            val createProjection = (exprs: Seq[Expression], attrs: Seq[Attribute]) =>
              MutableProjection.create(exprs, attrs)
            val tree = new WindowSegmentTree(
              Array[DeclarativeAggregate](Min(failure)), Seq(attr), createProjection,
              blockSize = 16, taskMemoryManager = TaskContext.get().taskMemoryManager())
            val processor = AggregateProcessor(
              Array[Expression](Sum(failure)), 0, Seq(attr), createProjection, Array(None))
            val frame = new UnboundedFollowingWindowFunctionFrame(
              new GenericInternalRow(1), processor, RowBoundOrdering(0))
            frame.prepare(rows)
            try {
              assert(rows.spillSize > 0)
              val run = if (segmentTree) () => tree.build(rows)
                else () => frame.write(0, InternalRow(0))
              Iterator.single(SpillReaderClosureSuite.failureGrowth(run))
            } finally {
              tree.close()
              rows.clear()
            }
          }.collect()
        }
        assert(observed.length == 1)
        assert(observed.head._1 == 16)
        info(s"Additional open files after caught failures: ${observed.head._2}")
        assert(observed.head._2 < 8)
      }
    }
  }

  private def checkFilesAndRows(
      plan: SparkPlan,
      expected: Seq[Row],
      segmentTree: Boolean = false): Unit = SQLExecution.withSQLConfPropagated(spark) {
    assume(openFileCount().isDefined, "needs a JVM that counts its open files")
    // Warm code generation and class loading before measuring file descriptors.
    plan.executeCollect()
    plan.resetMetrics()
    val dataTypes = plan.output.map(_.dataType)
    val observed = plan.execute().mapPartitions { iter =>
      val before = openFileCount().get
      var peak = before
      val rows = iter.map { row =>
        peak = math.max(peak, openFileCount().get)
        Row.fromSeq(row.toSeq(dataTypes))
      }.toList
      peak = math.max(peak, openFileCount().get)
      Iterator.single((rows, peak - before))
    }.collect()
    assert(observed.length == 1)
    val (rows, openedFiles) = observed.head
    info(s"Additional open files at row boundaries: $openedFiles")
    QueryTest.sameRows(expected, rows, isSorted = false).foreach(fail(_))
    val spillOperators = plan.collect {
      case join: SortMergeJoinExec => join
      case window: WindowExec => window
    }
    assert(spillOperators.nonEmpty)
    assert(spillOperators.forall(_.metrics("spillSize").value > 0))
    if (segmentTree) {
      val windows = plan.collect { case window: WindowExec => window }
      assert(windows.nonEmpty)
      assert(windows.forall(_.metrics("numSegmentTreeFrames").value > 0))
      assert(windows.forall(_.metrics("numSegmentTreeFallbackFrames").value == 0))
    }
    // Observe within the task, before its completion listeners close abandoned readers.
    assert(openedFiles < 8, s"the operator left $openedFiles more files open")
  }
}

private object SpillReaderClosureSuite {
  def failureGrowth(body: () => Unit): (Int, Long) = {
    def fails(): Boolean = {
      try {
        body()
        false
      } catch {
        case e: RuntimeException if Option(e.getMessage).exists(_.contains("expected failure")) =>
          true
      }
    }
    assert(fails())
    val before = openFileCount().get
    val failures = (0 until 16).count(_ => fails())
    (failures, openFileCount().get - before)
  }

  def openFileCount(): Option[Long] = ManagementFactory.getOperatingSystemMXBean match {
    case unix: UnixOperatingSystemMXBean => Some(unix.getOpenFileDescriptorCount)
    case _ => None
  }
}
