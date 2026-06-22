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

package org.apache.spark.sql.execution.exchange

import java.io.{InputStream => JInputStream, OutputStream => JOutputStream}
import java.util.UUID
import java.util.concurrent.{Future => JFuture, TimeUnit}

import scala.concurrent.{ExecutionContext, Promise, TimeoutException}
import scala.concurrent.duration.Duration

import org.apache.spark.{shard, SparkEnv, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.shard.BloomAccumulator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, ShardPartitioning}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.HashedRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.sketch.BloomFilter

trait ShardExchangeLike extends Exchange {
  def runId: UUID = UUID.randomUUID

  def relationFuture: JFuture[shard.ShardSetRef]

  final def submitShardJob: scala.concurrent.Future[shard.ShardSetRef] = executeQuery {
    completionFuture
  }

  protected def completionFuture: scala.concurrent.Future[shard.ShardSetRef]

  def runtimeStatistics: Statistics
}

/**
 * Shuffles the build side into shards, builds a [[HashedRelation]] per shard,
 * installs them on executors via [[ShardManager]], and constructs a bloom
 * filter for probe-side key filtering.
 */
case class ShardExchangeExec(
    buildBoundKeys: Seq[Expression],
    numShards: Int,
    replicaCount: Int,
    child: SparkPlan,
    filterExpr: Option[Expression] = None,
    filterSchema: Option[StructType] = None)
    extends ShardExchangeLike {

  override val runId: UUID = UUID.randomUUID

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = ShardPartitioning(buildBoundKeys, numShards)

  override protected def doCanonicalize(): SparkPlan = {
    copy(
      buildBoundKeys = buildBoundKeys.map(_.canonicalized),
      child = child.canonicalized,
      filterExpr = filterExpr.map(_.canonicalized))
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }

  @transient
  private lazy val promise = Promise[shard.ShardSetRef]()

  @transient
  override lazy val completionFuture: scala.concurrent.Future[shard.ShardSetRef] =
    promise.future

  @transient
  private val timeout: Long = conf.distributedMapJoinExchangeTimeout // seconds

  @transient
  override lazy val relationFuture: JFuture[shard.ShardSetRef] =
    SQLExecution
      .withThreadLocalCaptured[shard.ShardSetRef](session, ShardExchangeExec.executionContext) {
        var setId = -1L
        try {
          sparkContext.setJobGroup(
            runId.toString,
            s"shard exchange (runId $runId)",
            interruptOnCancel = true)
          if (!ensureExecutors(math.min(timeout / 2, 900), TimeUnit.SECONDS)) {
            logWarning(
              s"ensureExecutors timed out or SC stopped;" +
                s" active executors may be < target, continue build")
          }
          setId = sparkContext.env.shardManager.newShardSet(numShards, replicaCount)
          val shuffled = ShuffleExchangeExec(
            HashPartitioning(buildBoundKeys, numShards),
            child,
            REPARTITION_BY_NUM).execute()
          val sharded = new ShardedRowRDD(shuffled, getPreferredHosts(shuffled.partitions.length))
          val bloomCapacity = conf.getConf(SQLConf.DISTRIBUTED_MAP_JOIN_BLOOM_FILTER_CAPACITY)
          val filterBytes: Option[(Array[Byte], Array[Byte])] = filterExpr.map { expr =>
            val ser = SparkEnv.get.serializer.newInstance()
            val exprBytes = ser.serialize(expr).array()
            val schemaBytes = filterSchema
              .map(s => ser.serialize(s).array())
              .getOrElse(Array.empty[Byte])
            (exprBytes, schemaBytes)
          }
          val replicaTimeout = Duration(timeout, TimeUnit.SECONDS)
          val shardIds =
            sharded
              .mapPartitionsWithIndexInternal { case (shardId, rowIter) =>
                val bf = BloomFilter.create(bloomCapacity, 0.03d)
                val keyGenerator = UnsafeProjection.create(buildBoundKeys)
                val iter = rowIter.map { row =>
                  bf.put(keyGenerator(row).getBytes)
                  longMetric("numOutputRows").add(1)
                  row
                }
                val relation = HashedRelation(
                  iter,
                  buildBoundKeys,
                  taskMemoryManager = TaskContext.get().taskMemoryManager())

                TaskContext.get().addTaskCompletionListener[Unit](_ => relation.close())

                longMetric("dataSize").add(relation.estimatedSize)

                val sm = SparkEnv.get.shardManager
                sm.installShard(relation, setId, shardId, filterBytes)(bfOutput =>
                  bf.writeTo(bfOutput))
                sm.installReplicaSet(setId, shardId, replicaTimeout)

                Iterator.single(shardId)
              }
              .collect()
          // per shard-set
          val acc = new BloomAccumulator {
            private var bloom: BloomFilter = _
            override def add(input: JInputStream): Unit = {
              val b = BloomFilter.readFrom(input)
              if (bloom eq null) bloom = b else bloom.mergeInPlace(b)
            }
            override def isEmpty: Boolean = bloom eq null
            override def finish(output: JOutputStream): Unit = {
              if (!isEmpty) {
                bloom.writeTo(output)
              }
            }
          }
          SparkEnv.get.shardManager.mergeBloomFilter(setId, shardIds, acc)
          val setRef = shard.ShardSetRef(setId, shardIds)
          promise.trySuccess(setRef)
          sparkContext.cleaner.foreach(_.registerShardSetForCleanup(setRef))
          setRef
        } catch {
          case e: Throwable =>
            promise.tryFailure(e)
            if (setId >= 0) {
              try {
                sparkContext.env.shardManager.unpersist(setId, blocking = false)
              } catch {
                case scala.util.control.NonFatal(_) =>
              }
            }
            throw e
        }
      }

  override protected def doPrepare(): Unit = {
    // Initialize metrics
    metrics
    // Materialize the future.
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw QueryExecutionErrors.executeCodePathUnsupportedError("ShardExchange")

  def buildShardSet(): shard.ShardSetRef = try {
    relationFuture.get(timeout, TimeUnit.SECONDS)
  } catch {
    case ex: TimeoutException =>
      logError(s"Could not execute shard in $timeout seconds.", ex)
      if (!relationFuture.isDone) {
        sparkContext.cancelJobGroup(runId.toString)
        relationFuture.cancel(true)
      }
      throw new SparkException(s"shard exchange timeout.", ex)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ShardExchangeExec =
    copy(child = newChild)

  private def ensureExecutors(length: Long, unit: TimeUnit): Boolean = {
    val sparkConf = sparkContext.getConf
    if (Utils.isDynamicAllocationEnabled(sparkConf)) {
      val minExecutors =
        math.min(
          numShards,
          sparkConf.get(org.apache.spark.internal.config.DYN_ALLOCATION_MAX_EXECUTORS))
      try sparkContext.requestTotalExecutors(minExecutors, 0, Map.empty)
      catch { case scala.util.control.NonFatal(_) => () }

      val deadlineNs = System.nanoTime() + unit.toNanos(length)
      while (System.nanoTime() < deadlineNs && !sparkContext.isStopped) {
        val active =
          try sparkContext.getExecutorIds().size
          catch { case scala.util.control.NonFatal(_) => 0 }
        if (active == 0) {
          logWarning(
            "Active executors should not be 0, " +
              "spark.dynamicAllocation.minExecutors should be greater than 0.")
          return false
        }
        if (active >= minExecutors) return true
        Thread.sleep(10000)
      }
      false
    } else true
  }

  private def getPreferredHosts(prevPartLen: Int): Array[Seq[String]] = {
    def activeHosts: Array[String] = {
      val dh = sparkContext.conf.get("spark.driver.host", "")
      val infos = sparkContext.statusTracker.getExecutorInfos
      infos.iterator
        .map(_.host)
        .filter(_ != dh)
        .toArray
        .distinct
    }
    val hosts = activeHosts
    val shuffledHosts = scala.util.Random.shuffle(hosts.toSeq).toArray
    Array.tabulate(prevPartLen) { i =>
      if (shuffledHosts.nonEmpty) Seq(shuffledHosts(i % shuffledHosts.length)) else Nil
    }
  }
}

object ShardExchangeExec {

  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("shard-exchange", 8))
}
