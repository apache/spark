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

package org.apache.spark.sql.execution.benchmark

import java.util.UUID

import scala.collection.mutable
import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.operators.stateful.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.operators.stateful.join.{JoinStateManagerStoreGenerator, SymmetricHashJoinStateManager}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.LeftSide
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStoreConf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType, TimestampType}
import org.apache.spark.util.Utils


/**
 * Synthetic benchmark for Stream-Stream join operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to:
 *      "sql/core/benchmarks/StreamStreamJoinOneSideOperationsBenchmark-results.txt".
 * }}}
 */
object StreamStreamJoinOneSideOperationsBenchmark extends SqlBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runTestWithTimeWindowJoin()
    runTestWithTimeIntervalJoin()
  }

  case class StateOpInfo(
      queryRunId: UUID,
      checkpointLocation: String,
      operatorId: Int)

  private def runTestWithTimeWindowJoin(): Unit = {
    val (joinKeys, inputAttributes) = getAttributesForTimeWindowJoin()
    val stateFormatVersions = Seq(1, 2, 3)

    testWithTimeWindowJoin(
      inputAttributes = inputAttributes,
      joinKeys = joinKeys,
      stateFormatVersions = stateFormatVersions
    )
  }

  private def runTestWithTimeIntervalJoin(): Unit = {
    val (joinKeys, inputAttributes) = getAttributesForTimeIntervalJoin()
    val stateFormatVersions = Seq(1, 2, 3)

    testWithTimeIntervalJoin(
      inputAttributes = inputAttributes,
      joinKeys = joinKeys,
      stateFormatVersions = stateFormatVersions
    )
  }

  private def testWithTimeWindowJoin(
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      stateFormatVersions: Seq[Int]): Unit = {
    val numRowsInStateStore = 1000000

    val inputData = prepareInputDataForTimeWindowJoin(
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      numRows = numRowsInStateStore
    )

    val stateFormatVersionToStateOpInfo = mutable.HashMap[Int, StateOpInfo]()
    stateFormatVersions.foreach { stateFormatVersion =>
      val queryRunId = UUID.randomUUID()
      val checkpointDir = newDir()
      val operatorId = Random.nextInt()

      stateFormatVersionToStateOpInfo.put(
        stateFormatVersion,
        StateOpInfo(
          queryRunId = queryRunId,
          checkpointLocation = checkpointDir,
          operatorId = operatorId
        )
      )
    }

    testAppendWithTimeWindowJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo
    )

    testGetJoinedRowsWithTimeWindowJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      numKeysToGet = 100000
    )

    testEvictionRowsWithTimeWindowJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      // almost no eviction
      evictionRate = 0.0001
    )

    testEvictionRowsWithTimeWindowJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      evictionRate = 0.3
    )

    testEvictionRowsWithTimeWindowJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      evictionRate = 0.6
    )

    testEvictionRowsWithTimeWindowJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      evictionRate = 0.9
    )
  }

  private def testAppendWithTimeWindowJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      stateFormatVersionToStateOpInfo: mutable.HashMap[Int, StateOpInfo]): Unit = {
    val numInputRows = inputData.size
    val benchmarkForPut = new Benchmark(
      s"[Time-window Join] Append $numInputRows rows - ",
      numInputRows,
      minNumIters = 3,
      output = output)

    stateFormatVersions.foreach { stateFormatVersion =>
      val stateOpInfo = stateFormatVersionToStateOpInfo(stateFormatVersion)

      benchmarkForPut.addTimerCase(
        s"state format version: $stateFormatVersion") { timer =>

        val joinStateManager = createJoinStateManager(
          queryRunId = stateOpInfo.queryRunId,
          checkpointLocation = stateOpInfo.checkpointLocation,
          operatorId = stateOpInfo.operatorId,
          storeVersion = 0,
          inputAttributes = inputAttributes,
          joinKeys = joinKeys,
          stateFormatVersion = stateFormatVersion)

        timer.startTiming()
        inputData.foreach { case (keyRow, valueRow) =>
          joinStateManager.append(keyRow, valueRow, matched = false)
        }
        timer.stopTiming()

        joinStateManager.commit()
      }
    }

    benchmarkForPut.run()
  }

  private def testGetJoinedRowsWithTimeWindowJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      stateFormatVersionToStateOpInfo: mutable.HashMap[Int, StateOpInfo],
      numKeysToGet: Int): Unit = {

    val numRowsInStateStore = inputData.size

    val benchmarkForGetJoinedRows = new Benchmark(
      s"[Time-window Join] GetJoinedRows $numKeysToGet from $numRowsInStateStore rows",
      numKeysToGet,
      minNumIters = 3,
      output = output)

    val shuffledInputDataForJoinedRows = Random.shuffle(inputData).take(numKeysToGet)

    // FIXME: state format version 3 raises an exception
    //  Exception in thread "main" java.lang.NullPointerException:
    //  Cannot invoke "org.apache.spark.sql.catalyst.expressions.SpecializedGetters.getInt(int)"
    //  because "<parameter1>" is null
    //  Need to fix this after testing.
    stateFormatVersions.diff(Seq(3)).foreach { stateFormatVersion =>
      val stateOpInfo = stateFormatVersionToStateOpInfo(stateFormatVersion)

      benchmarkForGetJoinedRows.addTimerCase(
        s"state format version: $stateFormatVersion") { timer =>

        val joinStateManagerVer1 = createJoinStateManager(
          queryRunId = stateOpInfo.queryRunId,
          checkpointLocation = stateOpInfo.checkpointLocation,
          operatorId = stateOpInfo.operatorId,
          storeVersion = 1,
          inputAttributes = inputAttributes,
          joinKeys = joinKeys,
          stateFormatVersion = stateFormatVersion)

        val joinedRow = new JoinedRow()

        timer.startTiming()
        shuffledInputDataForJoinedRows.foreach { case (keyRow, valueRow) =>
          val joinedRows = joinStateManagerVer1.getJoinedRows(
            keyRow,
            thatRow => joinedRow.withLeft(valueRow).withRight(thatRow),
            _ => true,
            excludeRowsAlreadyMatched = false)
          joinedRows.foreach { _ =>
            // no-op, just to consume the iterator
          }
        }
        timer.stopTiming()

        joinStateManagerVer1.abortIfNeeded()
      }
    }

    benchmarkForGetJoinedRows.run()
  }

  private def testEvictionRowsWithTimeWindowJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      stateFormatVersionToStateOpInfo: mutable.HashMap[Int, StateOpInfo],
      evictionRate: Double): Unit = {

    assert(evictionRate >= 0.0 && evictionRate < 1.0,
      s"Eviction rate must be between 0.0 and 1.0, but got $evictionRate")

    val totalStateRows = inputData.size
    val numTargetEvictRows = (totalStateRows * evictionRate).toInt

    val orderedInputDataByTime = inputData.sortBy { case (keyRow, _) =>
      // extract window end time from the key row
      val windowRow = keyRow.getStruct(1, 2)
      windowRow.getLong(1)
    }

    val windowEndForEviction = orderedInputDataByTime.take(numTargetEvictRows).last
      ._1.getStruct(1, 2).getLong(1)

    val actualNumRows = inputData.count { case (keyRow, _) =>
      val windowRow = keyRow.getStruct(1, 2)
      val windowEnd = windowRow.getLong(1)
      windowEnd <= windowEndForEviction
    }

    val benchmarkForEviction = new Benchmark(
      s"[Time-window Join] Eviction with $actualNumRows from $totalStateRows rows",
      actualNumRows,
      minNumIters = 3,
      output = output)

    stateFormatVersions.foreach { stateFormatVersion =>
      val stateOpInfo = stateFormatVersionToStateOpInfo(stateFormatVersion)

      benchmarkForEviction.addTimerCase(
        s"state format version: $stateFormatVersion") { timer =>

        val joinStateManagerVer1 = createJoinStateManager(
          queryRunId = stateOpInfo.queryRunId,
          checkpointLocation = stateOpInfo.checkpointLocation,
          operatorId = stateOpInfo.operatorId,
          storeVersion = 1,
          inputAttributes = inputAttributes,
          joinKeys = joinKeys,
          stateFormatVersion = stateFormatVersion)

        timer.startTiming()
        val evictedRows = joinStateManagerVer1.removeByKeyCondition {
          keyRow =>
            val windowRow = keyRow.getStruct(1, 2)
            val windowEnd = windowRow.getLong(1)
            windowEnd <= windowEndForEviction
        }
        evictedRows.foreach { _ =>
          // no-op, just to consume the iterator
        }
        timer.stopTiming()

        joinStateManagerVer1.abortIfNeeded()
      }
    }

    benchmarkForEviction.run()
  }

  private def prepareInputDataForTimeWindowJoin(
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      numRows: Int): List[(UnsafeRow, UnsafeRow)] = {
    val rowsToAppend = mutable.ArrayBuffer[(UnsafeRow, UnsafeRow)]()
    val keyProj = UnsafeProjection.create(joinKeys, inputAttributes)
    val valueProj = UnsafeProjection.create(inputAttributes, inputAttributes)

    0.until(numRows).foreach { i =>
      val deptId = i % 10
      val windowStart = (i / 10) * 1000L
      val windowEnd = (i / 10) * 1000L + 1000L

      val sum = Random.nextInt(100)
      val count = Random.nextInt(10)

      val row = new GenericInternalRow(
        Array[Any](
          deptId,
          new GenericInternalRow(
            Array[Any](windowStart, windowEnd)
          ),
          sum.toLong,
          count.toLong)
      )

      val keyUnsafeRow = keyProj(row).copy()
      val valueUnsafeRow = valueProj(row).copy()
      rowsToAppend.append((keyUnsafeRow, valueUnsafeRow))
    }

    rowsToAppend.toList
  }

  private def testWithTimeIntervalJoin(
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      stateFormatVersions: Seq[Int]): Unit = {
    val numRowsInStateStore = 1000000

    val inputData = prepareInputDataForTimeIntervalJoin(
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      numRows = numRowsInStateStore
    )

    val stateFormatVersionToStateOpInfo = mutable.HashMap[Int, StateOpInfo]()
    stateFormatVersions.foreach { stateFormatVersion =>
      val queryRunId = UUID.randomUUID()
      val checkpointDir = newDir()
      val operatorId = Random.nextInt()

      stateFormatVersionToStateOpInfo.put(
        stateFormatVersion,
        StateOpInfo(
          queryRunId = queryRunId,
          checkpointLocation = checkpointDir,
          operatorId = operatorId
        )
      )
    }

    testAppendWithTimeIntervalJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo
    )

    testGetJoinedRowsWithTimeIntervalJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      numKeysToGet = 10000
    )

    testEvictionRowsWithTimeIntervalJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      // almost no eviction
      evictionRate = 0.0001
    )

    testEvictionRowsWithTimeIntervalJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      evictionRate = 0.3
    )

    testEvictionRowsWithTimeIntervalJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      evictionRate = 0.6
    )

    testEvictionRowsWithTimeIntervalJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      evictionRate = 0.9
    )
  }

  private def testAppendWithTimeIntervalJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      stateFormatVersionToStateOpInfo: mutable.HashMap[Int, StateOpInfo]): Unit = {
    val numInputRows = inputData.size
    val benchmarkForPut = new Benchmark(
      s"[Time-interval Join] Append $numInputRows rows",
      numInputRows,
      minNumIters = 3,
      output = output)

    stateFormatVersions.foreach { stateFormatVersion =>
      val stateOpInfo = stateFormatVersionToStateOpInfo(stateFormatVersion)

      benchmarkForPut.addTimerCase(
        s"state format version: $stateFormatVersion") { timer =>

        val joinStateManager = createJoinStateManager(
          queryRunId = stateOpInfo.queryRunId,
          checkpointLocation = stateOpInfo.checkpointLocation,
          operatorId = stateOpInfo.operatorId,
          storeVersion = 0,
          inputAttributes = inputAttributes,
          joinKeys = joinKeys,
          stateFormatVersion = stateFormatVersion)

        timer.startTiming()
        inputData.foreach { case (keyRow, valueRow) =>
          joinStateManager.append(keyRow, valueRow, matched = false)
        }
        timer.stopTiming()

        joinStateManager.commit()
      }
    }

    benchmarkForPut.run()
  }

  private def testGetJoinedRowsWithTimeIntervalJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      stateFormatVersionToStateOpInfo: mutable.HashMap[Int, StateOpInfo],
      numKeysToGet: Int): Unit = {

    val numRowsInStateStore = inputData.size

    val benchmarkForGetJoinedRows = new Benchmark(
      s"[Time-interval Join] GetJoinedRows $numKeysToGet from $numRowsInStateStore rows",
      numKeysToGet,
      minNumIters = 3,
      output = output)

    val shuffledInputDataForJoinedRows = Random.shuffle(inputData).take(numKeysToGet)

    // FIXME: state format version 3 raises an exception
    //  Exception in thread "main" java.lang.NullPointerException:
    //  Cannot invoke "org.apache.spark.sql.catalyst.expressions.SpecializedGetters.getInt(int)"
    //  because "<parameter1>" is null
    //  Need to fix this after testing.
    stateFormatVersions.diff(Seq(3)).foreach { stateFormatVersion =>
      val stateOpInfo = stateFormatVersionToStateOpInfo(stateFormatVersion)

      benchmarkForGetJoinedRows.addTimerCase(
        s"state format version: $stateFormatVersion") { timer =>

        val joinStateManagerVer1 = createJoinStateManager(
          queryRunId = stateOpInfo.queryRunId,
          checkpointLocation = stateOpInfo.checkpointLocation,
          operatorId = stateOpInfo.operatorId,
          storeVersion = 1,
          inputAttributes = inputAttributes,
          joinKeys = joinKeys,
          stateFormatVersion = stateFormatVersion)

        val joinedRow = new JoinedRow()

        timer.startTiming()
        shuffledInputDataForJoinedRows.foreach { case (keyRow, valueRow) =>
          val joinedRows = joinStateManagerVer1.getJoinedRows(
            keyRow,
            thatRow => joinedRow.withLeft(valueRow).withRight(thatRow),
            _ => true,
            excludeRowsAlreadyMatched = false)
          joinedRows.foreach { _ =>
            // no-op, just to consume the iterator
          }
        }
        timer.stopTiming()

        joinStateManagerVer1.abortIfNeeded()
      }
    }

    benchmarkForGetJoinedRows.run()
  }

  private def testEvictionRowsWithTimeIntervalJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      stateFormatVersionToStateOpInfo: mutable.HashMap[Int, StateOpInfo],
      evictionRate: Double): Unit = {

    assert(evictionRate >= 0.0 && evictionRate < 1.0,
      s"Eviction rate must be between 0.0 and 1.0, but got $evictionRate")

    val totalStateRows = inputData.size
    val numTargetEvictRows = (totalStateRows * evictionRate).toInt

    val orderedInputDataByTime = inputData.sortBy { case (_, valueRow) =>
      // impressionTimestamp is at index 1 in value row
      valueRow.getLong(1)
    }

    val tsForEviction = orderedInputDataByTime.take(numTargetEvictRows).last
      ._2.getLong(1)

    val actualNumRows = inputData.count { case (_, valueRow) =>
      valueRow.getLong(1) <= tsForEviction
    }

    val benchmarkForEviction = new Benchmark(
      s"[Time-interval Join] Eviction with $actualNumRows from $totalStateRows rows",
      actualNumRows,
      minNumIters = 3,
      output = output)

    // scalastyle:off line.size.limit
    // FIXME: state format version 3 has some issue with this -
    //  java.lang.NullPointerException: Cannot invoke
    //  "org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager$ValueAndMatchPair.value()"
    //  because "valuePair" is null
    // scalastyle:on line.size.limit
    stateFormatVersions.diff(Seq(3)).foreach { stateFormatVersion =>
      val stateOpInfo = stateFormatVersionToStateOpInfo(stateFormatVersion)

      benchmarkForEviction.addTimerCase(
        s"state format version: $stateFormatVersion") { timer =>

        val joinStateManagerVer1 = createJoinStateManager(
          queryRunId = stateOpInfo.queryRunId,
          checkpointLocation = stateOpInfo.checkpointLocation,
          operatorId = stateOpInfo.operatorId,
          storeVersion = 1,
          inputAttributes = inputAttributes,
          joinKeys = joinKeys,
          stateFormatVersion = stateFormatVersion)

        timer.startTiming()
        val evictedRows = joinStateManagerVer1.removeByValueCondition { valueRow =>
          valueRow.getLong(1) <= tsForEviction
        }
        evictedRows.foreach { _ =>
          // no-op, just to consume the iterator
        }
        timer.stopTiming()

        joinStateManagerVer1.abortIfNeeded()
      }
    }

    benchmarkForEviction.run()
  }

  private def prepareInputDataForTimeIntervalJoin(
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      numRows: Int): List[(UnsafeRow, UnsafeRow)] = {
    val rowsToAppend = mutable.ArrayBuffer[(UnsafeRow, UnsafeRow)]()
    val keyProj = UnsafeProjection.create(joinKeys, inputAttributes)
    val valueProj = UnsafeProjection.create(inputAttributes, inputAttributes)

    0.until(numRows).foreach { i =>
      val adId = i % 1000

      val impressionTimestamp = (i / 100) * 1000L

      // Below twos are effectively not used in join criteria and does not impact the benchmark
      val userId = Random.nextInt(1000)
      val campaignId = Random.nextInt(10000)

      val row = new GenericInternalRow(
        Array[Any](
          adId,
          impressionTimestamp,
          userId,
          campaignId)
      )

      val keyUnsafeRow = keyProj(row).copy()
      val valueUnsafeRow = valueProj(row).copy()
      rowsToAppend.append((keyUnsafeRow, valueUnsafeRow))
    }

    rowsToAppend.toList
  }

  private def getAttributesForTimeWindowJoin(): (Seq[Expression], Seq[Attribute]) = {
    val inputAttributes = Seq(
      AttributeReference("deptId", IntegerType, nullable = false)(),
      AttributeReference(
        "window",
        StructType(
          Seq(
            StructField("start", TimestampType, nullable = false),
            StructField("end", TimestampType, nullable = false))),
        nullable = false)(),
      AttributeReference("sum", LongType, nullable = false)(),
      AttributeReference("count", LongType, nullable = false)()
    )

    val joinKeys = Seq(
      inputAttributes(0), // deptId
      inputAttributes(1)  // window
    )

    (joinKeys, inputAttributes)
  }

  private def getAttributesForTimeIntervalJoin(): (Seq[Expression], Seq[Attribute]) = {
    val inputAttributes = Seq(
      AttributeReference("adId", IntegerType, nullable = false)(),
      AttributeReference("impressionTimestamp", TimestampType, nullable = false)(),
      AttributeReference("userId", IntegerType, nullable = false)(),
      AttributeReference("campaignId", IntegerType, nullable = false)()
    )
    val joinKeys = Seq(
      inputAttributes(0) // adId
    )
    (joinKeys, inputAttributes)
  }

  final def skip(benchmarkName: String)(func: => Any): Unit = {
    output.foreach(_.write(s"$benchmarkName is skipped".getBytes))
  }

  private def createJoinStateManager(
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      queryRunId: UUID,
      checkpointLocation: String,
      operatorId: Int,
      storeVersion: Int,
      stateFormatVersion: Int): SymmetricHashJoinStateManager = {
    val joinSide = LeftSide
    val partitionId = 0

    val stateInfo = Some(
      StatefulOperatorStateInfo(
        checkpointLocation = checkpointLocation,
        queryRunId = queryRunId,
        operatorId = operatorId,
        storeVersion = storeVersion,
        numPartitions = 1,
        stateSchemaMetadata = None,
        stateStoreCkptIds = None
      )
    )

    val sqlConf = new SQLConf()
    // We are only interested with RocksDB state store provider here
    sqlConf.setConfString("spark.sql.streaming.stateStore.providerClass",
      classOf[RocksDBStateStoreProvider].getName)
    sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
      true.toString)
    sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows",
      false.toString)
    sqlConf.setConfString("spark.sql.streaming.stateStore.coordinatorReportSnapshotUploadLag",
      false.toString)
    val storeConf = new StateStoreConf(sqlConf)

    val hadoopConf = new Configuration
    hadoopConf.set(StreamExecution.RUN_ID_KEY, queryRunId.toString)

    val joinStoreGenerator = new JoinStateManagerStoreGenerator()

    SymmetricHashJoinStateManager(
      joinSide = joinSide,
      inputValueAttributes = inputAttributes,
      joinKeys = joinKeys,
      stateInfo = stateInfo,
      storeConf = storeConf,
      hadoopConf = hadoopConf,
      partitionId = partitionId,
      keyToNumValuesStateStoreCkptId = None,
      keyWithIndexToValueStateStoreCkptId = None,
      snapshotOptions = None,
      stateFormatVersion = stateFormatVersion,
      skippedNullValueCount = None,
      joinStoreGenerator = joinStoreGenerator,
      useStateStoreCoordinator = false
    )
  }

  private def newDir(): String = Utils.createTempDir().toString
}
