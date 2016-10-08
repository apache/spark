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

package org.apache.spark.sql.execution.adaptive

import java.util.{HashMap => JHashMap, Map => JMap}
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.{MapOutputStatistics, ShuffleDependency, SimpleFutureAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{CollapseCodegenStages, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchange}
import org.apache.spark.sql.execution.joins._

/**
 * A physical plan tree is divided into a DAG tree of QueryFragment.
 * An QueryFragment is a basic execution unit that could be optimized
 * According to statistics of children fragments.
 */
trait QueryFragment extends SparkPlan {

  def children: Seq[QueryFragment]
  def isRoot: Boolean
  def id: Long

  private[this] var exchange: ShuffleExchange = null

  protected[this] var rootPlan: SparkPlan = null

  private[this] var fragmentInput: FragmentInput = null

  private[this] var parentFragment: QueryFragment = null

  var nextChildIndex: Int = 0

  private[this] val fragmentsIndex: JMap[QueryFragment, Integer] =
    new JHashMap[QueryFragment, Integer](children.size)

  private[this] val shuffleDependencies =
    new Array[ShuffleDependency[Int, InternalRow, InternalRow]](children.size)

  private[this] val mapOutputStatistics = new Array[MapOutputStatistics](children.size)

  private[this] val advisoryTargetPostShuffleInputSize: Long =
    sqlContext.conf.targetPostShuffleInputSize

  override def output: Seq[Attribute] = executedPlan.output

  private[this] def executedPlan: SparkPlan = if (isRoot) {
    rootPlan
  } else {
    exchange
  }

  private[sql] def setParentFragment(fragment: QueryFragment) = {
    this.parentFragment = fragment
  }

  private[sql] def getParentFragment() = parentFragment

  private[sql] def setFragmentInput(fragmentInput: FragmentInput) = {
    this.fragmentInput = fragmentInput
  }

  private[sql] def getFragmentInput() = fragmentInput

  private[sql] def setExchange(exchange: ShuffleExchange) = {
    this.exchange = exchange
  }

  private[sql] def getExchange(): ShuffleExchange = exchange

  private[sql] def setRootPlan(root: SparkPlan) = {
    this.rootPlan = root
  }

  protected[sql] def isAvailable: Boolean = synchronized {
    nextChildIndex >= children.size
  }

  private[sql] def codegenForExecution(plan: SparkPlan): SparkPlan = {
    CollapseCodegenStages(sqlContext.conf).apply(plan)
  }

  protected def doExecute(): RDD[InternalRow] = null

  protected[sql] def adaptiveExecute(): (ShuffleDependency[Int, InternalRow, InternalRow],
    SimpleFutureAction[MapOutputStatistics]) = synchronized {
    val executedPlan = codegenForExecution(exchange).asInstanceOf[ShuffleExchange]
    logInfo(s"== Submit Query Fragment ${id} Physical plan ==")
    logInfo(stringOrError(executedPlan.toString))
    val shuffleDependency = executedPlan.prepareShuffleDependency()
    if (shuffleDependency.rdd.partitions.length != 0) {
      val futureAction: SimpleFutureAction[MapOutputStatistics] =
        sqlContext.sparkContext.submitMapStage[Int, InternalRow, InternalRow](shuffleDependency)
      (shuffleDependency, futureAction)
    } else {
      (shuffleDependency, null)
    }
  }

  protected[sql] def stageFailed(exception: Throwable): Unit = synchronized {
    this.parentFragment.stageFailed(exception)
  }

  protected def stringOrError[A](f: => A): String =
    try f.toString catch { case e: Throwable => e.toString }

  protected[sql] def setChildCompleted(
      child: QueryFragment,
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      statistics: MapOutputStatistics): Unit = synchronized {
    fragmentsIndex.put(child, this.nextChildIndex)
    shuffleDependencies(this.nextChildIndex) = shuffleDependency
    mapOutputStatistics(this.nextChildIndex) = statistics
    this.nextChildIndex += 1
  }

  protected[sql] def optimizeOperator(): Unit = synchronized {
    val optimizedPlan = executedPlan.transformDown {
      case operator @ SortMergeJoinExec(leftKeys, rightKeys, _, _, left@SortExec(_, _, _, _),
          right@SortExec(_, _, _, _)) => {
        logInfo("Begin optimize join, operator =\n" + operator.toString)
        val newOperator = optimizeJoin(operator, left, right)
        logInfo("After optimize join, operator =\n" + newOperator.toString)
        newOperator
      }

      case agg @ TungstenAggregate(_, _, _, _, _, _, input @ FragmentInput(_))
          if (!input.isOptimized()) => {
        logInfo("Begin optimize agg, operator =\n" + agg.toString)
        optimizeAggregate(agg, input)
      }

      case operator: SparkPlan => operator
    }

    if (isRoot) {
      rootPlan = optimizedPlan
    } else {
      exchange = optimizedPlan.asInstanceOf[ShuffleExchange]
    }
  }

  private[this] def minNumPostShufflePartitions: Option[Int] = {
    val minNumPostShufflePartitions = sqlContext.conf.minNumPostShufflePartitions
    if (minNumPostShufflePartitions > 0) Some(minNumPostShufflePartitions) else None
  }

  private[this] def optimizeAggregate(agg: SparkPlan, input: FragmentInput): SparkPlan = {
    val childFragments = Seq(input.childFragment)
    val aggStatistics = new ArrayBuffer[MapOutputStatistics]()
    var i = 0
    while(i < childFragments.length) {
      val statistics = mapOutputStatistics(fragmentsIndex.get(childFragments(i)))
      if (statistics != null) {
        aggStatistics += statistics
      }
      i += 1
    }
    val partitionStartIndices =
      if (aggStatistics.length == 0) {
        None
      } else {
        Utils.estimatePartitionStartIndices(aggStatistics.toArray, minNumPostShufflePartitions,
          advisoryTargetPostShuffleInputSize)
      }
    val shuffledRowRdd = childFragments(0).getExchange().preparePostShuffleRDD(
      shuffleDependencies(fragmentsIndex.get(childFragments(0))), partitionStartIndices)
    childFragments(0).getFragmentInput().setShuffleRdd(shuffledRowRdd)
    childFragments(0).getFragmentInput().setOptimized()
    agg
  }

  private[this] def optimizeJoin(joinPlan: SortMergeJoinExec, left: SortExec, right: SortExec)
      : SparkPlan = {
    // TODO Optimize skew join
    val childFragments = Utils.findChildFragment(joinPlan)
    assert(childFragments.length == 2)
    val joinStatistics = new ArrayBuffer[MapOutputStatistics]()
    val childSizeInBytes = new Array[Long](childFragments.length)
    var i = 0
    while(i < childFragments.length) {
      val statistics = mapOutputStatistics(fragmentsIndex.get(childFragments(i)))
      if (statistics != null) {
        joinStatistics += statistics
        childSizeInBytes(i) = statistics.bytesByPartitionId.sum
      } else {
        childSizeInBytes(i) = 0
      }
      i += 1
    }
    val partitionStartIndices =
      if (joinStatistics.length == 0) {
        None
      } else {
        Utils.estimatePartitionStartIndices(joinStatistics.toArray, minNumPostShufflePartitions,
          advisoryTargetPostShuffleInputSize)
      }

    val leftFragment = childFragments(0)
    val rightFragment = childFragments(1)
    val leftShuffledRowRdd = leftFragment.getExchange().preparePostShuffleRDD(
      shuffleDependencies(fragmentsIndex.get(leftFragment)), partitionStartIndices)
    val rightShuffledRowRdd = rightFragment.getExchange().preparePostShuffleRDD(
      shuffleDependencies(fragmentsIndex.get(rightFragment)), partitionStartIndices)

    leftFragment.getFragmentInput().setShuffleRdd(leftShuffledRowRdd)
    leftFragment.getFragmentInput().setOptimized()
    rightFragment.getFragmentInput().setShuffleRdd(rightShuffledRowRdd)
    rightFragment.getFragmentInput().setOptimized()

    var newOperator: SparkPlan = joinPlan

    if (sqlContext.conf.autoBroadcastJoinThreshold > 0) {
      val leftSizeInBytes = childSizeInBytes(0)
      val rightSizeInBytes = childSizeInBytes(1)
      val joinType = joinPlan.joinType
      def canBuildLeft(joinType: JoinType): Boolean = joinType match {
        case Inner | RightOuter => true
        case _ => false
      }
      def canBuildRight(joinType: JoinType): Boolean = joinType match {
        case Inner | LeftOuter | LeftSemi | LeftAnti => true
        case j: ExistenceJoin => true
        case _ => false
      }
      if (leftSizeInBytes <= sqlContext.conf.autoBroadcastJoinThreshold && canBuildLeft(joinType)) {
        val keys = Utils.rewriteKeyExpr(joinPlan.leftKeys).map(
          BindReferences.bindReference(_, left.child.output))
        newOperator = BroadcastHashJoinExec(
          joinPlan.leftKeys,
          joinPlan.rightKeys,
          joinPlan.joinType,
          BuildLeft,
          joinPlan.condition,
          BroadcastExchangeExec(HashedRelationBroadcastMode(keys), left.child),
          right.child)
      } else if (rightSizeInBytes <= sqlContext.conf.autoBroadcastJoinThreshold
        && canBuildRight(joinType)) {
        val keys = Utils.rewriteKeyExpr(joinPlan.rightKeys).map(
          BindReferences.bindReference(_, right.child.output))
        newOperator = BroadcastHashJoinExec(
          joinPlan.leftKeys,
          joinPlan.rightKeys,
          joinPlan.joinType,
          BuildRight,
          joinPlan.condition,
          left.child,
          BroadcastExchangeExec(HashedRelationBroadcastMode(keys), right.child))
      }
    }
    newOperator
  }

  /** Returns a string representation of the nodes in this tree */
  override def treeString: String =
    executedPlan.generateTreeString(0, Nil, new StringBuilder).toString

  override def simpleString: String = "QueryFragment"
}

case class RootQueryFragment (
    children: Seq[QueryFragment],
    id: Long,
    isRoot: Boolean = false) extends QueryFragment {

  private[this] var isThrowException = false

  private[this] var exception: Throwable = null

  private[this] val stopped = new AtomicBoolean(false)

  override def nodeName: String = s"RootQueryFragment (fragment id: ${id})"

  protected[sql] override def stageFailed(exception: Throwable): Unit = {
    isThrowException = true
    this.exception = exception
    stopped.set(true)
  }

  private[this] val eventQueue: BlockingQueue[QueryFragment] =
    new LinkedBlockingDeque[QueryFragment]()

  protected[sql] def executeFragment(child: QueryFragment) = {
    val (shuffleDependency, futureAction) = child.adaptiveExecute()
    val parent = child.getParentFragment()
    if (futureAction != null) {
      futureAction.onComplete {
        case scala.util.Success(statistics) =>
          logInfo(s"Query Fragment ${id} finished")
          parent.setChildCompleted(child, shuffleDependency, statistics)
          if (parent.isAvailable) {
            logInfo(s"Query Fragment ${parent.id} is available")
            eventQueue.add(parent)
          }
        case scala.util.Failure(exception) =>
          logInfo(s"Query Fragment ${id} failed, exception is ${exception}")
          parent.stageFailed(exception)
      }
    } else {
      parent.setChildCompleted(child, shuffleDependency, null)
      if (parent.isAvailable) {
        logInfo(s"Query Fragment ${parent.id} is available")
        eventQueue.add(parent)
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    assert(isRoot == true)
    isThrowException = false
    stopped.set(false)
    val children = Utils.findLeafFragment(this)
    if (!children.isEmpty) {
      children.foreach { child => executeFragment(child) }
    } else {
      stopped.set(true)
    }

    val executeThread = new Thread("Fragment execute") {
      setDaemon(true)

      override def run(): Unit = {
        while (!stopped.get) {
          val fragment = eventQueue.take()
          fragment.optimizeOperator()
          if (fragment.isInstanceOf[RootQueryFragment]) {
            stopped.set(true)
          } else {
            executeFragment(fragment)
          }
        }
      }
    }
    executeThread.start()
    executeThread.join()
    if (isThrowException) {
      assert(this.exception != null)
      throw exception
    } else {
      logInfo(s"== Submit Query Fragment ${id} Physical plan ==")
      val executedPlan = codegenForExecution(rootPlan)
      logInfo(stringOrError(executedPlan.toString))
      executedPlan.execute()
    }
  }
}

case class UnaryQueryFragment (
    children: Seq[QueryFragment],
    id: Long,
    isRoot: Boolean = false) extends QueryFragment {

  override def nodeName: String = s"UnaryQueryFragment (fragment id: ${id})"
}
