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

package org.apache.spark.sql.execution.joins

import java.util

import scala.annotation.tailrec
import scala.concurrent.ExecutionContextExecutorService

import io.netty.buffer.Unpooled

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.shard.ShardSetRef
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, BindReferences, Expression, GenericInternalRow, JoinedRow, Predicate, PredicateHelper, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{DistributedMapJoinStrategy, JoinHint}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{BufferedShardRowMap, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ShardQueryStageExec
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShardExchangeExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.sketch.BloomFilter

/**
 * Physical operator for Distributed MapJoin.
 *
 * This strategy avoids full shuffle by building a distributed hash table service for the build
 * side (medium-sized table), and the probe side performs batched RPC lookups to complete the
 * join.
 *
 * Currently only supports:
 *   - Equi-join
 *   - BuildRight (right table as build side)
 *   - Explicit SQL hint: /*+distmapjoin(t(shard_count=5,replica_count=2))*/
 */
case class DistributedMapJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    hint: JoinHint,
    isNullAwareAntiJoin: Boolean = false)
    extends HashJoin with PredicateHelper {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val (numShards, replicaCount): (Int, Int) = {
    val strategy =
      (if (buildSide == BuildLeft) hint.leftHint else hint.rightHint).flatMap(_.strategy)
    strategy match {
      case Some(DistributedMapJoinStrategy(ns, rc)) => (ns.getOrElse(5), rc.getOrElse(1))
      case _ => (5, 1)
    }
  }

  @transient private lazy val (buildOnlyFilter, remainingCondition):
      (Option[Expression], Option[Expression]) = {
    condition match {
      case Some(cond) =>
        val buildAttrs = AttributeSet(buildOutput)
        val conjuncts = splitConjunctivePredicates(cond)
        val (bo, rem) = conjuncts.partition(_.references.subsetOf(buildAttrs))
        (bo.reduceOption(And), rem.reduceOption(And))
      case None => (None, None)
    }
  }

  @transient override protected[this] lazy val boundCondition: InternalRow => Boolean = {
    remainingCondition match {
      case Some(cond) =>
        Predicate.create(cond, streamedPlan.output ++ buildPlan.output).eval _
      case None =>
        (_: InternalRow) => true
    }
  }

  override def supportCodegen: Boolean = false

  override def supportsColumnar: Boolean = false

  override def needCopyResult: Boolean = false

  override def requiredChildDistribution: Seq[Distribution] = {
    val filter = buildOnlyFilter.map(BindReferences.bindReference(_, buildOutput))
    val filterSchema = buildOnlyFilter.map(_ => buildPlan.schema)
    val sd = ShardDistribution(buildBoundKeys, numShards, replicaCount, filter, filterSchema)
    buildSide match {
      case BuildLeft => Seq(sd, UnspecifiedDistribution)
      case BuildRight => Seq(UnspecifiedDistribution, sd)
    }
  }

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    throw QueryExecutionErrors.executeCodePathUnsupportedError("DistributedMapJoin")

  override protected def prepareRelation(ctx: CodegenContext): HashedRelationInfo =
    throw QueryExecutionErrors.executeCodePathUnsupportedError("DistributedMapJoin")

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val setRef = resolveShardSetRef(buildPlan)
    streamedPlan.execute().mapPartitionsInternal { streamedIter =>
      join(streamedIter, setRef.setId, numOutputRows)
    }
  }

  @tailrec
  private def resolveShardSetRef(plan: SparkPlan): ShardSetRef = plan match {
    case s: ShardExchangeExec => s.buildShardSet()
    case s: ShardQueryStageExec => s.shardSetRef
    case r: ReusedExchangeExec => resolveShardSetRef(r.child)
    case other =>
      throw new IllegalStateException(s"Unexpected build plan for DistributedMapJoin: $other")
  }

  private def streamedBloomFilter(setId: Long): BloomFilter = {
    SparkEnv.get.shardManager.fetchBloomFilter[BloomFilter](setId)(bfInput =>
      BloomFilter.readFrom(bfInput))
  }

  private def join(
      streamedIter: Iterator[InternalRow],
      setId: Long,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {

    val joinedRow = new JoinedRow

    val (scanBatch, onMissing): (
        BatchMatchReader => Iterator[InternalRow],
        InternalRow => Option[InternalRow]) = joinType match {
      case _: InnerLike =>
        (innerJoinScan(_, joinedRow), (_: InternalRow) => None)
      case LeftOuter | RightOuter =>
        val nullRow = new GenericInternalRow(buildOutput.length)
        (outerJoinScan(_, joinedRow, nullRow),
          (sr: InternalRow) => Some(joinedRow.withLeft(sr).withRight(nullRow)))
      case LeftSemi =>
        (semiJoinScan(_, joinedRow), (_: InternalRow) => None)
      case LeftAnti =>
        (antiJoinScan(_, joinedRow), (sr: InternalRow) => Some(sr))
      case _: ExistenceJoin =>
        val existsRow = new GenericInternalRow(Array[Any](null))
        (existenceJoinScan(_, joinedRow, existsRow),
          (sr: InternalRow) => { existsRow.setBoolean(0, false); Some(joinedRow(sr, existsRow)) })
      case x =>
        throw new IllegalArgumentException(
          s"DistributedMapJoin should not take $x as the JoinType")
    }

    val iter = new LookupJoinIterator(streamedIter, setId, scanBatch, onMissing)
    val resultProj = createResultProjection()
    iter.map { row =>
      numOutputRows.add(1)
      resultProj(row)
    }
  }

  // ---------------------------------------------------------------------------
  // Scan methods: one per join type, each self-contained
  // ---------------------------------------------------------------------------

  private def innerJoinScan(
      reader: BatchMatchReader,
      joinedRow: JoinedRow): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var _has = false
      override def hasNext: Boolean = { if (!_has) _has = advance(); _has }
      override def next(): InternalRow = { _has = false; joinedRow }

      private def advance(): Boolean = {
        while (true) {
          val br = reader.nextBuildRow()
          if (br != null) {
            if (boundCondition(joinedRow.withLeft(reader.curStreamed).withRight(br))) return true
          } else if (!reader.advanceStreamedRow()) {
            return false
          }
        }
        false // unreachable
      }
    }
  }

  private def outerJoinScan(
      reader: BatchMatchReader,
      joinedRow: JoinedRow,
      nullRow: InternalRow): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var found = false
      private var _has = false
      override def hasNext: Boolean = { if (!_has) _has = advance(); _has }
      override def next(): InternalRow = { _has = false; joinedRow }

      private def advance(): Boolean = {
        while (true) {
          val br = reader.nextBuildRow()
          if (br != null) {
            if (boundCondition(joinedRow.withLeft(reader.curStreamed).withRight(br))) {
              found = true
              return true
            }
          } else {
            if (!found && reader.curStreamed != null) {
              joinedRow.withLeft(reader.curStreamed).withRight(nullRow)
              found = true
              return true
            }
            if (!reader.advanceStreamedRow()) return false
            found = false
          }
        }
        false // unreachable
      }
    }
  }

  private def semiJoinScan(
      reader: BatchMatchReader,
      joinedRow: JoinedRow): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var _has = false
      override def hasNext: Boolean = { if (!_has) _has = advance(); _has }
      override def next(): InternalRow = { _has = false; reader.curStreamed }

      private def advance(): Boolean = {
        while (reader.advanceStreamedRow()) {
          var br = reader.nextBuildRow()
          while (br != null) {
            if (boundCondition(joinedRow.withLeft(reader.curStreamed).withRight(br))) {
              reader.skipRemainingBuildRows()
              return true
            }
            br = reader.nextBuildRow()
          }
        }
        false
      }
    }
  }

  private def antiJoinScan(
      reader: BatchMatchReader,
      joinedRow: JoinedRow): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var _has = false
      override def hasNext: Boolean = { if (!_has) _has = advance(); _has }
      override def next(): InternalRow = { _has = false; reader.curStreamed }

      private def advance(): Boolean = {
        while (reader.advanceStreamedRow()) {
          if (findMatchingBuildRow(reader, joinedRow)) {
            reader.skipRemainingBuildRows()
          } else {
            return true
          }
        }
        false
      }
    }
  }

  private def existenceJoinScan(
      reader: BatchMatchReader,
      joinedRow: JoinedRow,
      existsRow: GenericInternalRow): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var _has = false
      override def hasNext: Boolean = { if (!_has) _has = advance(); _has }
      override def next(): InternalRow = { _has = false; joinedRow }

      private def advance(): Boolean = {
        if (!reader.advanceStreamedRow()) return false
        val exists = findMatchingBuildRow(reader, joinedRow)
        if (exists) reader.skipRemainingBuildRows()
        existsRow.setBoolean(0, exists)
        joinedRow.withLeft(reader.curStreamed).withRight(existsRow)
        true
      }
    }
  }

  @tailrec
  private def findMatchingBuildRow(
      reader: BatchMatchReader,
      joinedRow: JoinedRow): Boolean = {
    val br = reader.nextBuildRow()
    if (br == null) false
    else if (boundCondition(joinedRow.withLeft(reader.curStreamed).withRight(br))) true
    else findMatchingBuildRow(reader, joinedRow)
  }

  // ---------------------------------------------------------------------------
  // BatchMatchReader: reads batch response buffer, zero-copy
  // ---------------------------------------------------------------------------

  private type PBatch = BufferedShardRowMap#KeyValueBatch
  private type BBuffer = ManagedBuffer

  private class BatchMatchReader(batch: PBatch, buffer: BBuffer) extends AutoCloseable {
    private val keyIter = batch.multiValuesIterator()
    private val buf = Unpooled.wrappedBuffer(buffer.nioByteBuffer())
    private val buildUr: UnsafeRow = new UnsafeRow(buildOutput.length)
    private val advanceRead = UnsafeRowBufCodec.makeAdvanceRead(buildUr, buf)
    private var streamedIter: java.util.Iterator[UnsafeRow] = _
    private var buildRowsStart: Int = -1
    private var atSentinel = true
    var curStreamed: UnsafeRow = _

    assert(batch.getSetId == buf.readLong(), "setId mismatch")
    assert(batch.getShard == buf.readInt(), "shardId mismatch")

    def advanceStreamedRow(): Boolean = {
      if (streamedIter != null && streamedIter.hasNext) {
        curStreamed = streamedIter.next()
        rewindBuildRows()
        true
      } else {
        skipRemainingBuildRows()
        if (!keyIter.hasNext) return false
        streamedIter = keyIter.next()
        buildRowsStart = buf.readerIndex()
        atSentinel = false
        if (!streamedIter.hasNext) advanceStreamedRow()
        else { curStreamed = streamedIter.next(); true }
      }
    }

    def nextBuildRow(): UnsafeRow = {
      if (atSentinel) return null
      val blen = buf.readInt()
      if (blen == 0) { atSentinel = true; null }
      else advanceRead(blen)
    }

    def skipRemainingBuildRows(): Unit = {
      while (!atSentinel) {
        val blen = buf.readInt()
        if (blen == 0) atSentinel = true
        else buf.readerIndex(buf.readerIndex() + blen)
      }
    }

    private def rewindBuildRows(): Unit = {
      buf.readerIndex(buildRowsStart)
      atSentinel = false
    }

    override def close(): Unit = {
      batch.release()
      buffer.release()
    }
  }

  // ---------------------------------------------------------------------------
  // LookupJoinIterator: async pipeline management
  // ---------------------------------------------------------------------------

  private class LookupJoinIterator(
      streamedIter: Iterator[InternalRow],
      setId: Long,
      scanBatch: BatchMatchReader => Iterator[InternalRow],
      onMissing: InternalRow => Option[InternalRow])
      extends Iterator[InternalRow] {

    private val maxInFlightNum = conf.distributedMapJoinMaxInFlightNum
    private val keyGenerator: UnsafeProjection = UnsafeProjection.create(streamedBoundKeys)
    private val valueGenerator: UnsafeProjection = UnsafeProjection.create(streamedPlan.schema)
    private val shardGenerator =
      UnsafeProjection.create(
        HashPartitioning(streamedBoundKeys, numShards).partitionIdExpression :: Nil)

    private val bloom = streamedBloomFilter(setId)
    private val probeUr: UnsafeRow = new UnsafeRow(streamedPlan.schema.length)
    private val bufferedMap = {
      val mm = TaskContext.get().taskMemoryManager()
      val maxBatchSize = conf.distributedMapJoinMaxBatchSize
      val map =
        new BufferedShardRowMap(
          mm,
          setId,
          numShards,
          streamedBoundKeys.length,
          probeUr,
          maxBatchSize)
      TaskContext.get().addTaskCompletionListener[Unit](_ => map.free())
      map
    }

    private sealed trait Lookup
    private case class LookupSuccess(batch: PBatch, buffer: BBuffer) extends Lookup
    private case class LookupFailure(batch: PBatch, cause: Throwable) extends Lookup

    private val lookupQueue = new util.concurrent.LinkedBlockingQueue[Lookup]

    private var inputExhausted = false
    private var prepared = false
    private var nextRowVal: InternalRow = _
    private var numInFlight = 0
    private var currentReader: BatchMatchReader = _
    private var currentBatchIter: Iterator[InternalRow] = _

    override def hasNext: Boolean = {
      if (!prepared) {
        processNext()
      }
      prepared
    }

    override def next(): InternalRow = {
      if (!prepared && !hasNext) {
        throw QueryExecutionErrors.noSuchElementExceptionError()
      }
      prepared = false
      nextRowVal
    }

    private def prepareNextRow(row: InternalRow): Unit = {
      nextRowVal = row
      prepared = true
    }

    private def processNext(): Unit = {
      if (currentBatchIter != null) {
        iterateLookup()
      }

      var hasLookup = true
      while (!prepared && hasLookup) {
        val ele = lookupQueue.poll()
        if (ele == null) {
          hasLookup = false
        } else {
          pollLookup(ele)
          if (currentBatchIter != null) {
            iterateLookup()
          }
        }
      }

      while (!prepared && !inputExhausted && streamedIter.hasNext) {
        val streamedRow = streamedIter.next()
        val keyUr = keyGenerator(streamedRow)
        if (keyUr.anyNull || !bloom.mightContain(keyUr.getBytes)) {
          onMissing(streamedRow).foreach(prepareNextRow)
        } else {
          val shardId = shardGenerator(streamedRow).getInt(0)
          val valueUr = streamedRow match {
            case ur: UnsafeRow => ur
            case r => valueGenerator(r)
          }
          processLookup(shardId, keyUr, valueUr)
        }
      }

      if (!prepared) {
        if (!inputExhausted) {
          inputExhausted = true
          flushLookup(bufferedMap.tailingIterator())
        }
        while (!prepared && numInFlight > 0) {
          pollLookup(lookupQueue.poll(200, util.concurrent.TimeUnit.MILLISECONDS))
          if (currentBatchIter != null) {
            iterateLookup()
          }
        }
      }
    }

    private def processLookup(shardId: Int, keyUr: UnsafeRow, valueUr: UnsafeRow): Unit = {
      bufferedMap.putRow(
        shardId,
        keyUr.getBaseObject,
        keyUr.getBaseOffset,
        keyUr.getSizeInBytes,
        keyUr.hashCode(),
        valueUr.getBaseObject,
        valueUr.getBaseOffset,
        valueUr.getSizeInBytes)

      if (bufferedMap.hasPending) {
        flushLookup(bufferedMap.pendingIterator())
      }

      while (numInFlight >= maxInFlightNum) {
        pollLookup(lookupQueue.poll(200, util.concurrent.TimeUnit.MILLISECONDS))
        if (currentBatchIter != null) {
          iterateLookup()
        }
      }
    }

    private def flushLookup[T <: PBatch](iter: util.Iterator[T]): Unit = {
      val manager = SparkEnv.get.shardManager
      implicit val ec: ExecutionContextExecutorService = manager.lookupEc
      while (iter.hasNext) {
        val batch = iter.next()
        iter.remove()
        val future =
          manager.fetchRemoteBatch(setId, batch.getShard, () => batch.wrapKeysBuffer())

        future.onComplete {
          case scala.util.Success(buffer) =>
            lookupQueue.put(LookupSuccess(batch, buffer))
          case scala.util.Failure(cause) =>
            lookupQueue.put(LookupFailure(batch, cause))
        }

        numInFlight += 1
      }
    }

    private def pollLookup(look: Lookup): Unit = {
      look match {
        case LookupSuccess(batch, buffer) =>
          currentReader = new BatchMatchReader(batch, buffer)
          currentBatchIter = scanBatch(currentReader)
          numInFlight -= 1
        case LookupFailure(batch, cause) =>
          throw new SparkException(
            s"DistributedMapJoin batch lookup failed: ($setId, ${batch.getShard}) ",
            cause)
        case null =>
      }
    }

    private def iterateLookup(): Unit = {
      if (currentBatchIter.hasNext) {
        prepareNextRow(currentBatchIter.next())
      } else {
        currentReader.close()
        currentReader = null
        currentBatchIter = null
      }
    }
  }
}
