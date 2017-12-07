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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

import org.apache.spark.{broadcast, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{Predicate => GenPredicate, _}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.ThreadUtils

/**
 * The base class for physical operators.
 *
 * The naming convention is that physical operators end with "Exec" suffix, e.g. [[ProjectExec]].
 */
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient
  final val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when we are being deserialized on the slaves.  In this instance
  // the value of subexpressionEliminationEnabled will be set by the deserializer after the
  // constructor has run.
  val subexpressionEliminationEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.subexpressionEliminationEnabled
  } else {
    false
  }

  /** Overridden make copy also propagates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    SparkSession.setActiveSession(sqlContext.sparkSession)
    super.makeCopy(newArgs)
  }

  /**
   * Return all metadata that describes more details of this SparkPlan.
   */
  def metadata: Map[String, String] = Map.empty

  /**
   * Return all metrics containing metrics of this SparkPlan.
   */
  def metrics: Map[String, SQLMetric] = Map.empty

  /**
   * Reset all the metrics.
   */
  def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
  }

  /**
   * Return a LongSQLMetric according to the name.
   */
  def longMetric(name: String): SQLMetric = metrics(name)

  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /** Specifies any partition requirements on the input data for this operator. */
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
    doExecute()
  }

  /**
   * Returns the result of this query as a broadcast variable by delegating to `doExecuteBroadcast`
   * after preparations.
   *
   * Concrete implementations of SparkPlan should override `doExecuteBroadcast`.
   */
  final def executeBroadcast[T](): broadcast.Broadcast[T] = executeQuery {
    doExecuteBroadcast()
  }

  /**
   * Execute a query after preparing the query and adding query plan information to created RDDs
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
   * Prepare a SparkPlan for execution. It's idempotent.
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
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
   *
   * This will only be called once, protected by `this`.
   */
  protected def doPrepare(): Unit = {}

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as a broadcast variable.
   */
  protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    throw new UnsupportedOperationException(s"$nodeName does not implement doExecuteBroadcast")
  }

  /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   */
  private def getByteArrayRdd(n: Int = -1): RDD[Array[Byte]] = {
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))
      while (iter.hasNext && (n < 0 || count < n)) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        count += 1
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator(bos.toByteArray)
    }
  }

  /**
   * Decode the byte arrays back to UnsafeRows and put them into buffer.
   */
  private def decodeUnsafeRows(bytes: Array[Byte]): Iterator[InternalRow] = {
    val nFields = schema.length

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(codec.compressedInputStream(bis))

    new Iterator[InternalRow] {
      private var sizeOfNextRow = ins.readInt()
      override def hasNext: Boolean = sizeOfNextRow >= 0
      override def next(): InternalRow = {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(nFields)
        row.pointTo(bs, sizeOfNextRow)
        sizeOfNextRow = ins.readInt()
        row
      }
    }
  }

  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[InternalRow] = {
    val byteArrayRdd = getByteArrayRdd()

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { bytes =>
      decodeUnsafeRows(bytes).foreach(results.+=)
    }
    results.toArray
  }

  /**
   * Runs this query returning the result as an iterator of InternalRow.
   *
   * Note: this will trigger multiple jobs (one for each partition).
   */
  def executeToIterator(): Iterator[InternalRow] = {
    getByteArrayRdd().toLocalIterator.flatMap(decodeUnsafeRows)
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
   * This is modeled after RDD.take but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = getByteArrayRdd(n)

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, quadruple and retry.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate
        // it by 50%. We also cap the estimation in the end.
        val limitScaleUpFactor = Math.max(sqlContext.conf.limitScaleUpFactor, 2)
        if (buf.isEmpty) {
          numPartsToTry = partsScanned * limitScaleUpFactor
        } else {
          // the left side of max is >=1 whenever partsScanned >= 2
          numPartsToTry = Math.max((1.5 * n * partsScanned / buf.size).toInt - partsScanned, 1)
          numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
        }
      }

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD,
        (it: Iterator[Array[Byte]]) => if (it.hasNext) it.next() else Array.empty[Byte], p)

      buf ++= res.flatMap(decodeUnsafeRows)

      partsScanned += p.size
    }

    if (buf.size > n) {
      buf.take(n).toArray
    } else {
      buf.toArray
    }
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute],
      useSubexprElimination: Boolean = false): MutableProjection = {
    log.debug(s"Creating MutableProj: $expressions, inputSchema: $inputSchema")
    GenerateMutableProjection.generate(expressions, inputSchema, useSubexprElimination)
  }

  private def genInterpretedPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): InterpretedPredicate = {
    val str = expression.toString
    val logMessage = if (str.length > 256) {
      str.substring(0, 256 - 3) + "..."
    } else {
      str
    }
    logWarning(s"Codegen disabled for this expression:\n $logMessage")
    InterpretedPredicate.create(expression, inputSchema)
  }

  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): GenPredicate = {
    try {
      GeneratePredicate.generate(expression, inputSchema)
    } catch {
      case e @ (_: InternalCompilerException | _: CompileException)
          if sqlContext == null || sqlContext.conf.wholeStageFallback =>
        genInterpretedPredicate(expression, inputSchema)
    }
  }

  protected def newOrdering(
      order: Seq[SortOrder], inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    GenerateOrdering.generate(order, inputSchema)
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

object SparkPlan {
  private[execution] val subqueryExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("subquery", 16))
}

trait LeafExecNode extends SparkPlan {
  override final def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

object UnaryExecNode {
  def unapply(a: Any): Option[(SparkPlan, SparkPlan)] = a match {
    case s: SparkPlan if s.children.size == 1 => Some((s, s.children.head))
    case _ => None
  }
}

trait UnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil
}

trait BinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)
}
