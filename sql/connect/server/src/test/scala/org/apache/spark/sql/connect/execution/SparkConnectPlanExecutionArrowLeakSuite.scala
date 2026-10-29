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

package org.apache.spark.sql.connect.execution

import java.nio.channels.ClosedByInterruptException
import java.util.UUID

import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver
import org.apache.arrow.memory.BufferAllocator
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkArithmeticException
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Tail}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.{DataFrame, Dataset}
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.connect.service.{ExecuteKey, SparkConnectService}
import org.apache.spark.sql.execution.{CollectLimitExec, CollectTailExec, LocalTableScanExec}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Checks that `SparkConnectPlanExecution.processAsArrowBatches` and
 * `SparkConnectPlanner.handleSqlCommand` release the Arrow memory of the converter when they
 * convert rows on the driver, where no TaskContext closes it, and the conversion or sending fails.
 */
class SparkConnectPlanExecutionArrowLeakSuite extends SharedSparkSession with SparkConnectPlanTest {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
    SparkConnectService.sessionManager.initializeBaseSession(() => spark.newSession())
  }

  private val schema = new StructType().add("id", LongType).add("s", StringType)

  // 2600000 hours fit in CalendarInterval microseconds, but overflow Arrow's int64 nanoseconds.
  private val bigInterval = "make_interval(0, 0, 0, 0, 2600000, 0, 0)"

  private def converterAllocators(): Seq[BufferAllocator] =
    ArrowUtils.rootAllocator.getChildAllocators.asScala.toSeq
      .filter(_.getName == "toArrowBatchWithSchemaIterator")

  private case class Outcome(error: Option[Throwable], leaked: Seq[BufferAllocator], bytes: Long)

  /** Runs `body` and returns its error and the converter allocators and bytes it leaves behind. */
  private def measure(body: => Unit): Outcome = {
    val before = converterAllocators()
    val beforeBytes = ArrowUtils.rootAllocator.getAllocatedMemory
    val error = try {
      body
      None
    } catch {
      case t: Throwable => Some(t)
    } finally {
      Thread.interrupted()
    }
    val leaked = converterAllocators().filterNot(a => before.exists(_ eq a))
    val bytes = ArrowUtils.rootAllocator.getAllocatedMemory - beforeBytes
    Outcome(error, leaked, bytes)
  }

  /** A row that interrupts the thread reading it, like an interrupt during the row loop. */
  private class InterruptingRow(values: Array[Any]) extends GenericInternalRow(values) {
    // Armed after planning: test-only analyzer validation also reads LocalRelation rows.
    @volatile var armed = false
    override def getLong(ordinal: Int): Long = {
      if (armed) Thread.currentThread().interrupt()
      super.getLong(ordinal)
    }
  }

  private def rowValues(i: Int): Array[Any] = Array(i.toLong, UTF8String.fromString(s"row-$i"))

  private def localDf(numRows: Int, interruptAtRow: Int = -1): DataFrame = {
    val toUnsafe = UnsafeProjection.create(schema)
    val rows = (0 until numRows).map { i =>
      if (i == interruptAtRow) {
        new InterruptingRow(rowValues(i))
      } else {
        toUnsafe(new GenericInternalRow(rowValues(i))).copy()
      }
    }
    Dataset.ofRows(spark, LocalRelation(DataTypeUtils.toAttributes(schema), rows))
  }

  private def rangeDf: DataFrame =
    spark.range(0, 1000, 1, 4).selectExpr("id", "CAST(id AS STRING) AS s")

  private def tailDf: DataFrame = Dataset.ofRows(spark, Tail(Literal(500), rangeDf.logicalPlan))

  /** Calls processAsArrowBatches directly; `onArrowBatch` runs for each Arrow batch sent. */
  private def runArrowBatches(df: DataFrame)(onArrowBatch: Int => Unit): Unit = {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val holder = SparkConnectTestUtils.createDummyExecuteHolder(
      sessionHolder, proto.Command.getDefaultInstance)
    var numBatches = 0
    val observer = new StreamObserver[proto.ExecutePlanResponse] {
      override def onNext(r: proto.ExecutePlanResponse): Unit = if (r.hasArrowBatch) {
        numBatches += 1
        onArrowBatch(numBatches)
      }
      override def onError(t: Throwable): Unit = {}
      override def onCompleted(): Unit = {}
    }
    new SparkConnectPlanExecution(holder).processAsArrowBatches(df, observer, holder)
  }

  private def assertNoLeak(outcome: Outcome): Unit = {
    assert(outcome.leaked.isEmpty, s"${outcome.leaked.size} converter allocators left open")
    assert(outcome.bytes == 0, s"${outcome.bytes} bytes left in the root allocator")
  }

  Seq[(String, () => DataFrame, Class[_])](
    ("LocalTableScanExec", () => localDf(1000), classOf[LocalTableScanExec]),
    ("CollectLimitExec", () => rangeDf.limit(500), classOf[CollectLimitExec]),
    ("CollectTailExec", () => tailDf, classOf[CollectTailExec])).foreach {
    case (name, df, planClass) =>
      test(s"SPARK-60736: $name: interrupt after the first sent batch") {
        withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "100") {
          val frame = df()
          assert(planClass.isInstance(frame.queryExecution.executedPlan))
          val outcome = measure {
            runArrowBatches(frame) { n => if (n == 1) Thread.currentThread().interrupt() }
          }
          assert(outcome.error.exists(_.isInstanceOf[ClosedByInterruptException]))
          assertNoLeak(outcome)
        }
      }

      test(s"SPARK-60736: $name: failure while sending the first batch") {
        withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "100") {
          val outcome = measure {
            runArrowBatches(df()) { n => if (n == 1) throw new IllegalStateException("send") }
          }
          assert(outcome.error.exists(_.getMessage == "send"))
          assertNoLeak(outcome)
        }
      }

      test(s"SPARK-60736: $name: complete conversion (control)") {
        withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "100") {
          val outcome = measure(runArrowBatches(df())(_ => ()))
          assert(outcome.error.isEmpty)
          assertNoLeak(outcome)
        }
      }
  }

  test("SPARK-60736: LocalTableScanExec: interrupt while filling a batch") {
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "100") {
      val df = localDf(1000, interruptAtRow = 50)
      val row = df.queryExecution.executedPlan.asInstanceOf[LocalTableScanExec].rows(50)
        .asInstanceOf[InterruptingRow]
      Thread.interrupted()
      row.armed = true
      val outcome = measure(runArrowBatches(df)(_ => ()))
      assert(outcome.error.exists(_.isInstanceOf[ClosedByInterruptException]))
      assertNoLeak(outcome)
    }
  }

  test("SPARK-60736: LocalTableScanExec: ArrowWriter error") {
    val df = spark.sql(s"SELECT * FROM VALUES ($bigInterval) AS t(i)")
    assert(df.queryExecution.executedPlan.isInstanceOf[LocalTableScanExec])
    val outcome = measure(runArrowBatches(df)(_ => ()))
    assert(outcome.error.exists(_.isInstanceOf[SparkArithmeticException]))
    assertNoLeak(outcome)
  }

  test("SPARK-60736: CollectLimitExec: ArrowWriter error") {
    val df = spark.sql(s"SELECT $bigInterval AS i FROM range(10) LIMIT 5")
    assert(df.queryExecution.executedPlan.isInstanceOf[CollectLimitExec])
    val outcome = measure(runArrowBatches(df)(_ => ()))
    assert(outcome.error.exists(_.isInstanceOf[SparkArithmeticException]))
    assertNoLeak(outcome)
  }

  test("SPARK-60736: empty LocalTableScanExec (control)") {
    val outcome = measure(runArrowBatches(localDf(0))(_ => ()))
    assert(outcome.error.isEmpty)
    assertNoLeak(outcome)
  }

  test("SPARK-60736: ArrowBatchWithSchemaIterator: close after a failure while serializing " +
    "the batch") {
    val toUnsafe = UnsafeProjection.create(schema)
    def rows(interruptAt: Int): Iterator[InternalRow] = Iterator.tabulate(100) { i =>
      if (i == interruptAt) Thread.currentThread().interrupt()
      toUnsafe(new GenericInternalRow(rowValues(i))).copy()
    }
    // -1: interrupted before next(), so the schema write fails before the batch exists.
    // 50: interrupted while filling the batch, so MessageSerializer.serialize(batch) fails.
    Seq(-1, 50).foreach { interruptAt =>
      val outcome = measure {
        val batches = ArrowConverters.toBatchWithSchemaIterator(
          rows(interruptAt), schema, 100, 0, "UTC",
          errorOnDuplicatedFieldNames = false, largeVarTypes = false)
        if (interruptAt < 0) Thread.currentThread().interrupt()
        Utils.tryWithSafeFinally(batches.next())(batches.close())
      }
      assert(outcome.error.exists(_.isInstanceOf[ClosedByInterruptException]))
      assert(outcome.error.get.getSuppressed.isEmpty, s"interruptAt=$interruptAt")
      assertNoLeak(outcome)
    }
  }

  /** Waits until the execution thread of `operationId` is blocked on adding a response. */
  private def waitUntilBlockedOnNext(operationId: String): Unit = {
    val thread = Thread.getAllStackTraces.keySet.asScala
      .find(_.getName.endsWith(s"_opId=$operationId"))
      .get
    Eventually.eventually(Eventually.timeout(60.seconds)) {
      assert(thread.getState == Thread.State.BLOCKED &&
        thread.getStackTrace.exists(_.getMethodName == "tryOnNext"))
    }
  }

  private def newRequest(plan: proto.Plan): proto.ExecutePlanRequest = proto.ExecutePlanRequest
    .newBuilder()
    .setPlan(plan)
    .setUserContext(proto.UserContext.newBuilder().setUserId("c1"))
    .setSessionId(UUID.randomUUID.toString)
    .build()

  /** Executes `plan` through `SparkConnectService` and returns the error the client receives. */
  private def executePlan(plan: proto.Plan): Option[Throwable] = {
    @volatile var done = false
    @volatile var error = Option.empty[Throwable]
    new SparkConnectService(false).executePlan(
      newRequest(plan),
      new StreamObserver[proto.ExecutePlanResponse] {
        override def onNext(r: proto.ExecutePlanResponse): Unit = {}
        override def onError(t: Throwable): Unit = {
          error = Some(t)
          done = true
        }
        override def onCompleted(): Unit = done = true
      })
    Eventually.eventually(Eventually.timeout(60.seconds)) {
      assert(done)
    }
    error
  }

  /**
   * Executes `plan` and interrupts it through the Interrupt RPC while its execution thread is
   * blocked on adding the schema response. For a local relation, nothing before the driver-side
   * Arrow conversion observes the interrupt, so its first write always raises it. Returns the
   * error that the client receives.
   */
  private def executeAndInterrupt(plan: proto.Plan): Option[Throwable] = {
    val instance = new SparkConnectService(false)
    val request = newRequest(plan)
    val sessionHolder = SparkConnectService.getOrCreateIsolatedSession(
      request.getUserContext.getUserId, request.getSessionId, None)
    val holder = SparkConnectService.executionManager.createExecuteHolder(
      ExecuteKey(request, sessionHolder), request, sessionHolder)
    holder.responseObserver.responseLock.synchronized {
      holder.start()
      waitUntilBlockedOnNext(holder.operationId)
      instance.interrupt(
        proto.InterruptRequest
          .newBuilder()
          .setSessionId(request.getSessionId)
          .setUserContext(request.getUserContext)
          .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)
          .build(),
        new StreamObserver[proto.InterruptResponse] {
          override def onNext(v: proto.InterruptResponse): Unit = {}
          override def onError(t: Throwable): Unit = {}
          override def onCompleted(): Unit = {}
        })
    }
    var error = Option.empty[Throwable]
    holder.runGrpcResponseSender(new ExecuteGrpcResponseSender[proto.ExecutePlanResponse](
      holder,
      new StreamObserver[proto.ExecutePlanResponse] {
        override def onNext(r: proto.ExecutePlanResponse): Unit = {}
        override def onError(t: Throwable): Unit = error = Some(t)
        override def onCompleted(): Unit = {}
      }))
    error
  }

  private def sqlRoot(query: String): proto.Plan = proto.Plan
    .newBuilder()
    .setRoot(proto.Relation.newBuilder().setSql(proto.SQL.newBuilder().setQuery(query)))
    .build()

  test("SPARK-60736: E2E: ArrowWriter error in a local relation") {
    var error = Option.empty[Throwable]
    val outcome = measure {
      error = executePlan(sqlRoot(s"SELECT * FROM VALUES ($bigInterval) AS t(i)"))
    }
    assert(error.exists(_.getMessage.contains("DATETIME_OVERFLOW")))
    assertNoLeak(outcome)
  }

  test("SPARK-60736: E2E: ArrowWriter error in a SQL script result (handleSqlCommand)") {
    val plan = proto.Plan
      .newBuilder()
      .setCommand(
        proto.Command
          .newBuilder()
          .setSqlCommand(
            proto.SqlCommand
              .newBuilder()
              .setInput(proto.Relation
                .newBuilder()
                .setSql(proto.SQL.newBuilder().setQuery(s"BEGIN SELECT $bigInterval AS i; END")))))
      .build()
    var error = Option.empty[Throwable]
    val outcome = measure {
      error = executePlan(plan)
    }
    assert(error.exists(_.getMessage.contains("DATETIME_OVERFLOW")))
    assertNoLeak(outcome)
  }

  test("SPARK-60736: E2E: Interrupt RPC for a LocalRelation result") {
    val toUnsafe = UnsafeProjection.create(schema)
    val relation = createLocalRelationProto(
      schema, (0 until 1000).map(i => toUnsafe(new GenericInternalRow(rowValues(i))).copy()))
    var error = Option.empty[Throwable]
    val outcome = measure {
      error = executeAndInterrupt(proto.Plan.newBuilder().setRoot(relation).build())
    }
    assert(error.exists(_.getMessage.contains("OPERATION_CANCELED")))
    assertNoLeak(outcome)
  }
}
