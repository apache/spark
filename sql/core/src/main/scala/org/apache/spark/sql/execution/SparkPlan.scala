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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.spark.{broadcast, SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, TreeNodeTag, UnaryLike}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.NextIterator

object SparkPlan {
  /** The original [[LogicalPlan]] from which this [[SparkPlan]] is converted. */
  val LOGICAL_PLAN_TAG = TreeNodeTag[LogicalPlan]("logical_plan")

  /** The [[LogicalPlan]] inherited from its ancestor. */
  val LOGICAL_PLAN_INHERITED_TAG = TreeNodeTag[LogicalPlan]("logical_plan_inherited")

  private val nextPlanId = new AtomicInteger(0)

  /** Register a new SparkPlan, returning its SparkPlan ID */
  private[execution] def newPlanId(): Int = nextPlanId.getAndIncrement()
}

/**
 * The base class for physical operators.
 *
 * The naming convention is that physical operators end with "Exec" suffix, e.g. [[ProjectExec]].
 */
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {
  @transient final val session = SparkSession.getActiveSession.orNull

  protected def sparkContext = session.sparkContext

  override def conf: SQLConf = {
    if (session != null) {
      session.sessionState.conf
    } else {
      super.conf
    }
  }

  val id: Int = SparkPlan.newPlanId()

  /**
   * Return true if this stage of the plan supports row-based execution. A plan
   * can also support columnar execution (see `supportsColumnar`). Spark will decide
   * which execution to be called during query planning.
   */
  def supportsRowBased: Boolean = !supportsColumnar

  /**
   * Return true if this stage of the plan supports columnar execution. A plan
   * can also support row-based execution (see `supportsRowBased`). Spark will decide
   * which execution to be called during query planning.
   */
  def supportsColumnar: Boolean = false

  /**
   * The exact java types of the columns that are output in columnar processing mode. This
   * is a performance optimization for code generation and is optional.
   */
  def vectorTypes: Option[Seq[String]] = None

  /** Overridden make copy also propagates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    if (session != null) {
      session.withActive(super.makeCopy(newArgs))
    } else {
      super.makeCopy(newArgs)
    }
  }

  /**
   * @return The logical plan this plan is linked to.
   */
  def logicalLink: Option[LogicalPlan] =
    getTagValue(SparkPlan.LOGICAL_PLAN_TAG)
      .orElse(getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG))

  /**
   * Set logical plan link recursively if unset.
   */
  def setLogicalLink(logicalPlan: LogicalPlan): Unit = {
    setLogicalLink(logicalPlan, false)
  }

  private def setLogicalLink(logicalPlan: LogicalPlan, inherited: Boolean = false): Unit = {
    // Stop at a descendant which is the root of a sub-tree transformed from another logical node.
    if (inherited && getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isDefined) {
      return
    }

    val tag = if (inherited) {
      SparkPlan.LOGICAL_PLAN_INHERITED_TAG
    } else {
      SparkPlan.LOGICAL_PLAN_TAG
    }
    setTagValue(tag, logicalPlan)
    children.foreach(_.setLogicalLink(logicalPlan, true))
  }

  /**
   * @return All metrics containing metrics of this SparkPlan.
   */
  def metrics: Map[String, SQLMetric] = Map.empty

  /**
   * Resets all the metrics.
   */
  def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
    children.foreach(_.resetMetrics())
  }

  /**
   * @return [[SQLMetric]] for the `name`.
   */
  def longMetric(name: String): SQLMetric = metrics(name)

  // TODO: Move to `DistributedPlan`
  /**
   * Specifies how data is partitioned across different nodes in the cluster.
   * Note this method may fail if it is invoked before `EnsureRequirements` is applied
   * since `PartitioningCollection` requires all its partitionings to have
   * the same number of partitions.
   */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /**
   * Specifies the data distribution requirements of all the children for this operator. By default
   * it's [[UnspecifiedDistribution]] for each child, which means each child can have any
   * distribution.
   *
   * If an operator overwrites this method, and specifies distribution requirements(excluding
   * [[UnspecifiedDistribution]] and [[BroadcastDistribution]]) for more than one child, Spark
   * guarantees that the outputs of these children will have same number of partitions, so that the
   * operator can safely zip partitions of these children's result RDDs. Some operators can leverage
   * this guarantee to satisfy some interesting requirement, e.g., non-broadcast joins can specify
   * HashClusteredDistribution(a,b) for its left child, and specify HashClusteredDistribution(c,d)
   * for its right child, then it's guaranteed that left and right child are co-partitioned by
   * a,b/c,d, which means tuples of same value are in the partitions of same index, e.g.,
   * (a=1,b=2) and (c=1,d=2) are both in the second partition of left and right child.
   */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies how data is ordered in each partition. */
  def outputOrdering: Seq[SortOrder] = Nil

  /** Specifies sort order for each partition requirements on the input data for this operator. */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to `doExecute` after
   * preparations.
   *
   * Concrete implementations of SparkPlan should override `doExecute`.
   */
  final def execute(): RDD[InternalRow] = executeQuery {
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecute()
  }

  /**
   * Returns the result of this query as a broadcast variable by delegating to `doExecuteBroadcast`
   * after preparations.
   *
   * Concrete implementations of SparkPlan should override `doExecuteBroadcast`.
   */
  final def executeBroadcast[T](): broadcast.Broadcast[T] = executeQuery {
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteBroadcast()
  }

  /**
   * Returns the result of this query as an RDD[ColumnarBatch] by delegating to `doColumnarExecute`
   * after preparations.
   *
   * Concrete implementations of SparkPlan should override `doColumnarExecute` if `supportsColumnar`
   * returns true.
   */
  final def executeColumnar(): RDD[ColumnarBatch] = executeQuery {
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteColumnar()
  }

  /**
   * Executes a query after preparing the query and adding query plan information to created RDDs
   * for visualization.
   */
  protected final def executeQuery[T](query: => T): T = {
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      waitForSubqueries()
      query
    }
  }

  /**
   * List of (uncorrelated scalar subquery, future holding the subquery result) for this plan node.
   * This list is populated by [[prepareSubqueries]], which is called in [[prepare]].
   */
  @transient
  private val runningSubqueries = new ArrayBuffer[ExecSubqueryExpression]

  /**
   * Finds scalar subquery expressions in this plan node and starts evaluating them.
   */
  protected def prepareSubqueries(): Unit = {
    expressions.foreach {
      _.collect {
        case e: ExecSubqueryExpression =>
          e.plan.prepare()
          runningSubqueries += e
      }
    }
  }

  /**
   * Blocks the thread until all subqueries finish evaluation and update the results.
   */
  protected def waitForSubqueries(): Unit = synchronized {
    // fill in the result of subqueries
    runningSubqueries.foreach { sub =>
      sub.updateResult()
    }
    runningSubqueries.clear()
  }

  /**
   * Whether the "prepare" method is called.
   */
  private var prepared = false

  /**
   * Prepares this SparkPlan for execution. It's idempotent.
   */
  final def prepare(): Unit = {
    // doPrepare() may depend on it's children, we should call prepare() on all the children first.
    children.foreach(_.prepare())
    synchronized {
      if (!prepared) {
        prepareSubqueries()
        doPrepare()
        prepared = true
      }
    }
  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
   *
   * @note `prepare` method has already walked down the tree, so the implementation doesn't have
   * to call children's `prepare` methods.
   *
   * This will only be called once, protected by `this`.
   */
  protected def doPrepare(): Unit = {}

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * Produces the result of the query as a broadcast variable.
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    throw QueryExecutionErrors.doExecuteBroadcastNotImplementedError(nodeName)
  }

  /**
   * Produces the result of the query as an `RDD[ColumnarBatch]` if [[supportsColumnar]] returns
   * true. By convention the executor that creates a ColumnarBatch is responsible for closing it
   * when it is no longer needed. This allows input formats to be able to reuse batches if needed.
   */
  protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw SparkException.internalError(s"Internal Error ${this.getClass} has column support" +
      s" mismatch:\n${this}")
  }

  /**
   * Converts the output of this plan to row-based if it is columnar plan.
   */
  def toRowBased: SparkPlan = if (supportsColumnar) ColumnarToRowExec(this) else this

  /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   */
  private def getByteArrayRdd(
      n: Int = -1, takeFromEnd: Boolean = false): RDD[(Long, Array[Byte])] = {
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))

      if (takeFromEnd && n > 0) {
        // To collect n from the last, we should anyway read everything with keeping the n.
        // Otherwise, we don't know where is the last from the iterator.
        var last: Seq[UnsafeRow] = Seq.empty[UnsafeRow]
        val slidingIter = iter.map(_.copy()).sliding(n)
        while (slidingIter.hasNext) { last = slidingIter.next().asInstanceOf[Seq[UnsafeRow]] }
        var i = 0
        count = last.length
        while (i < count) {
          val row = last(i)
          out.writeInt(row.getSizeInBytes)
          row.writeToStream(out, buffer)
          i += 1
        }
      } else {
        // `iter.hasNext` may produce one row and buffer it, we should only call it when the
        // limit is not hit.
        while ((n < 0 || count < n) && iter.hasNext) {
          val row = iter.next().asInstanceOf[UnsafeRow]
          out.writeInt(row.getSizeInBytes)
          row.writeToStream(out, buffer)
          count += 1
        }
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator((count, bos.toByteArray))
    }
  }

  /**
   * Decodes the byte arrays back to UnsafeRows and put them into buffer.
   */
  private def decodeUnsafeRows(bytes: Array[Byte]): Iterator[InternalRow] = {
    val nFields = schema.length

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(codec.compressedInputStream(bis))

    new NextIterator[InternalRow] {
      private var sizeOfNextRow = ins.readInt()
      private def _next(): InternalRow = {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(nFields)
        row.pointTo(bs, sizeOfNextRow)
        sizeOfNextRow = ins.readInt()
        row
      }

      override def getNext(): InternalRow = {
        if (sizeOfNextRow >= 0) {
          try {
            _next()
          } catch {
            case t: Throwable if ins != null =>
              ins.close()
              throw t
          }
        } else {
          finished = true
          null
        }
      }
      override def close(): Unit = ins.close()
    }
  }

  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[InternalRow] = {
    val byteArrayRdd = getByteArrayRdd()

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { countAndBytes =>
      decodeUnsafeRows(countAndBytes._2).foreach(results.+=)
    }
    results.toArray
  }

  private[spark] def executeCollectIterator(): (Long, Iterator[InternalRow]) = {
    val countsAndBytes = getByteArrayRdd().collect()
    val total = countsAndBytes.map(_._1).sum
    val rows = countsAndBytes.iterator.flatMap(countAndBytes => decodeUnsafeRows(countAndBytes._2))
    (total, rows)
  }

  /**
   * Runs this query returning the result as an iterator of InternalRow.
   *
   * @note Triggers multiple jobs (one for each partition).
   */
  def executeToIterator(): Iterator[InternalRow] = {
    getByteArrayRdd().map(_._2).toLocalIterator.flatMap(decodeUnsafeRows)
  }

  /**
   * Runs this query returning the result as an array, using external Row format.
   */
  def executeCollectPublic(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    executeCollect().map(converter(_).asInstanceOf[Row])
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   *
   * This is modeled after `RDD.take` but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[InternalRow] = executeTake(n, takeFromEnd = false)

  /**
   * Runs this query returning the last `n` rows as an array.
   *
   * This is modeled after `RDD.take` but never runs any job locally on the driver.
   */
  def executeTail(n: Int): Array[InternalRow] = executeTake(n, takeFromEnd = true)

  private def executeTake(n: Int, takeFromEnd: Boolean): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = getByteArrayRdd(n, takeFromEnd)

    val buf = if (takeFromEnd) new ListBuffer[InternalRow] else new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.length < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, quadruple and retry.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate
        // it by 50%. We also cap the estimation in the end.
        val limitScaleUpFactor = Math.max(conf.limitScaleUpFactor, 2)
        if (buf.isEmpty) {
          numPartsToTry = partsScanned * limitScaleUpFactor
        } else {
          val left = n - buf.length
          // As left > 0, numPartsToTry is always >= 1
          numPartsToTry = Math.ceil(1.5 * left * partsScanned / buf.length).toInt
          numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
        }
      }

      val parts = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val partsToScan = if (takeFromEnd) {
        // Reverse partitions to scan. So, if parts was [1, 2, 3] in 200 partitions (0 to 199),
        // it becomes [198, 197, 196].
        parts.map(p => (totalParts - 1) - p)
      } else {
        parts
      }
      val sc = sparkContext
      val res = sc.runJob(childRDD, (it: Iterator[(Long, Array[Byte])]) =>
        if (it.hasNext) it.next() else (0L, Array.emptyByteArray), partsToScan)

      var i = 0

      if (takeFromEnd) {
        while (buf.length < n && i < res.length) {
          val rows = decodeUnsafeRows(res(i)._2)
          if (n - buf.length >= res(i)._1) {
            buf.prepend(rows.toArray[InternalRow]: _*)
          } else {
            val dropUntil = res(i)._1 - (n - buf.length)
            // Same as Iterator.drop but this only takes a long.
            var j: Long = 0L
            while (j < dropUntil) { rows.next(); j += 1L}
            buf.prepend(rows.toArray[InternalRow]: _*)
          }
          i += 1
        }
      } else {
        while (buf.length < n && i < res.length) {
          val rows = decodeUnsafeRows(res(i)._2)
          if (n - buf.length >= res(i)._1) {
            buf ++= rows.toArray[InternalRow]
          } else {
            buf ++= rows.take(n - buf.length).toArray[InternalRow]
          }
          i += 1
        }
      }
      partsScanned += partsToScan.size
    }
    buf.toArray
  }

  /**
   * Cleans up the resources used by the physical operator (if any). In general, all the resources
   * should be cleaned up when the task finishes but operators like SortMergeJoinExec and LimitExec
   * may want eager cleanup to free up tight resources (e.g., memory).
   */
  protected[sql] def cleanupResources(): Unit = {
    children.foreach(_.cleanupResources())
  }
}

trait LeafExecNode extends SparkPlan with LeafLike[SparkPlan] {

  override def producedAttributes: AttributeSet = outputSet
  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val outputStr = s"${ExplainUtils.generateFieldString("Output", output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$outputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$outputStr
         |""".stripMargin
    }
  }
}

object UnaryExecNode {
  def unapply(a: Any): Option[(SparkPlan, SparkPlan)] = a match {
    case s: SparkPlan if s.children.size == 1 => Some((s, s.children.head))
    case _ => None
  }
}

trait UnaryExecNode extends SparkPlan with UnaryLike[SparkPlan] {

  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val inputStr = s"${ExplainUtils.generateFieldString("Input", child.output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$inputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$inputStr
         |""".stripMargin
    }
  }
}

trait BinaryExecNode extends SparkPlan with BinaryLike[SparkPlan] {

  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val leftOutputStr = s"${ExplainUtils.generateFieldString("Left output", left.output)}"
    val rightOutputStr = s"${ExplainUtils.generateFieldString("Right output", right.output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$leftOutputStr
         |$rightOutputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$leftOutputStr
         |$rightOutputStr
         |""".stripMargin
    }
  }
}
