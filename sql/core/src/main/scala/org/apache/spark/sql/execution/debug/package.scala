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

import java.util.Collections

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.{ByteCodeStats, CodeFormatter, CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.util.StringConcat
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

/**
 * Contains methods for debugging query execution.
 *
 * Usage:
 * {{{
 *   import org.apache.spark.sql.execution.debug._
 *   sql("SELECT 1").debug()
 *   sql("SELECT 1").debugCodegen()
 * }}}
 *
 * or for streaming case (structured streaming):
 * {{{
 *   import org.apache.spark.sql.execution.debug._
 *   val query = df.writeStream.<...>.start()
 *   query.debugCodegen()
 * }}}
 *
 * Note that debug in structured streaming is not supported, because it doesn't make sense for
 * streaming to execute batch once while main query is running concurrently.
 */
package object debug {

  /** Helper function to evade the println() linter. */
  private def debugPrint(msg: String): Unit = {
    // scalastyle:off println
    println(msg)
    // scalastyle:on println
  }

  /**
   * Get WholeStageCodegenExec subtrees and the codegen in a query plan into one String
   *
   * @param plan the query plan for codegen
   * @return single String containing all WholeStageCodegen subtrees and corresponding codegen
   */
  def codegenString(plan: SparkPlan): String = {
    val concat = new StringConcat()
    writeCodegen(concat.append, plan)
    concat.toString
  }

  def writeCodegen(append: String => Unit, plan: SparkPlan): Unit = {
    val codegenSeq = codegenStringSeq(plan)
    append(s"Found ${codegenSeq.size} WholeStageCodegen subtrees.\n")
    for (((subtree, code, codeStats), i) <- codegenSeq.zipWithIndex) {
      val usedConstPoolRatio = if (codeStats.maxConstPoolSize > 0) {
        val rt = 100.0 * codeStats.maxConstPoolSize / CodeGenerator.MAX_JVM_CONSTANT_POOL_SIZE
        "(%.2f%% used)".format(rt)
      } else {
        ""
      }
      val codeStatsStr = s"maxMethodCodeSize:${codeStats.maxMethodCodeSize}; " +
        s"maxConstantPoolSize:${codeStats.maxConstPoolSize}$usedConstPoolRatio; " +
        s"numInnerClasses:${codeStats.numInnerClasses}"
      append(s"== Subtree ${i + 1} / ${codegenSeq.size} ($codeStatsStr) ==\n")
      append(subtree)
      append("\nGenerated code:\n")
      append(s"$code\n")
    }
  }

  /**
   * Get WholeStageCodegenExec subtrees and the codegen in a query plan
   *
   * @param plan the query plan for codegen
   * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
   */
  def codegenStringSeq(plan: SparkPlan): Seq[(String, String, ByteCodeStats)] = {
    val codegenSubtrees = new collection.mutable.HashSet[WholeStageCodegenExec]()

    def findSubtrees(plan: SparkPlan): Unit = {
      plan foreach {
        case s: WholeStageCodegenExec =>
          codegenSubtrees += s
        case p: AdaptiveSparkPlanExec =>
          // Find subtrees from current executed plan of AQE.
          findSubtrees(p.executedPlan)
        case s: QueryStageExec =>
          findSubtrees(s.plan)
        case s =>
          s.subqueries.foreach(findSubtrees)
      }
    }

    findSubtrees(plan)
    codegenSubtrees.toSeq.sortBy(_.codegenStageId).map { subtree =>
      val (_, source) = subtree.doCodeGen()
      val codeStats = try {
        CodeGenerator.compile(source)._2
      } catch {
        case NonFatal(_) =>
          ByteCodeStats.UNAVAILABLE
      }
      (subtree.toString, CodeFormatter.format(source), codeStats)
    }
  }

  /**
   * Get WholeStageCodegenExec subtrees and the codegen in a query plan into one String
   *
   * @param query the streaming query for codegen
   * @return single String containing all WholeStageCodegen subtrees and corresponding codegen
   */
  def codegenString(query: StreamingQuery): String = {
    val w = asStreamExecution(query)
    if (w.lastExecution != null) {
      codegenString(w.lastExecution.executedPlan)
    } else {
      "No physical plan. Waiting for data."
    }
  }

  /**
   * Get WholeStageCodegenExec subtrees and the codegen in a query plan
   *
   * @param query the streaming query for codegen
   * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
   */
  def codegenStringSeq(query: StreamingQuery): Seq[(String, String, ByteCodeStats)] = {
    val w = asStreamExecution(query)
    if (w.lastExecution != null) {
      codegenStringSeq(w.lastExecution.executedPlan)
    } else {
      Seq.empty
    }
  }

  private def asStreamExecution(query: StreamingQuery): StreamExecution = query match {
    case wrapper: StreamingQueryWrapper => wrapper.streamingQuery
    case q: StreamExecution => q
    case _ => throw new IllegalArgumentException("Parameter should be an instance of " +
      "StreamExecution!")
  }

  /**
   * Augments [[Dataset]]s with debug methods.
   */
  implicit class DebugQuery(query: Dataset[_]) extends Logging {
    def debug(): Unit = {
      val visited = new collection.mutable.HashSet[TreeNodeRef]()
      val debugPlan = query.queryExecution.executedPlan transform {
        case s: SparkPlan if !visited.contains(new TreeNodeRef(s)) =>
          visited += new TreeNodeRef(s)
          DebugExec(s)
      }
      debugPrint(s"Results returned: ${debugPlan.execute().count()}")
      debugPlan.foreach {
        case d: DebugExec => d.dumpStats()
        case _ =>
      }
    }

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def debugCodegen(): Unit = {
      debugPrint(codegenString(query.queryExecution.executedPlan))
    }
  }

  implicit class DebugStreamQuery(query: StreamingQuery) extends Logging {
    def debugCodegen(): Unit = {
      debugPrint(codegenString(query))
    }
  }


  class SetAccumulator[T] extends AccumulatorV2[T, java.util.Set[T]] {
    private val _set = Collections.synchronizedSet(new java.util.HashSet[T]())

    override def isZero: Boolean = _set.isEmpty

    override def copy(): AccumulatorV2[T, java.util.Set[T]] = {
      val newAcc = new SetAccumulator[T]()
      newAcc._set.addAll(_set)
      newAcc
    }

    override def reset(): Unit = _set.clear()

    override def add(v: T): Unit = _set.add(v)

    override def merge(other: AccumulatorV2[T, java.util.Set[T]]): Unit = {
      _set.addAll(other.value)
    }

    override def value: java.util.Set[T] = _set
  }

  case class DebugExec(child: SparkPlan) extends UnaryExecNode with CodegenSupport {
    def output: Seq[Attribute] = child.output

    /**
     * A collection of metrics for each column of output.
     */
    case class ColumnMetrics() {
      val elementTypes = new SetAccumulator[String]
      sparkContext.register(elementTypes)
    }

    val tupleCount: LongAccumulator = sparkContext.longAccumulator

    val numColumns: Int = child.output.size
    val columnStats: Array[ColumnMetrics] = Array.fill(child.output.size)(new ColumnMetrics())

    def dumpStats(): Unit = {
      debugPrint(s"== ${child.simpleString(SQLConf.get.maxToStringFields)} ==")
      debugPrint(s"Tuples output: ${tupleCount.value}")
      child.output.zip(columnStats).foreach { case (attr, metric) =>
        // This is called on driver. All accumulator updates have a fixed value. So it's safe to use
        // `asScala` which accesses the internal values using `java.util.Iterator`.
        val actualDataTypes = metric.elementTypes.value.asScala.mkString("{", ",", "}")
        debugPrint(s" ${attr.name} ${attr.dataType}: $actualDataTypes")
      }
    }

    protected override def doExecute(): RDD[InternalRow] = {
      val evaluatorFactory = new DebugEvaluatorFactory(tupleCount, numColumns,
        columnStats.map(_.elementTypes), output)
      if (conf.usePartitionEvaluator) {
        child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        child.execute().mapPartitionsWithIndex { (index, iter) =>
          evaluatorFactory.createEvaluator().eval(index, iter)
        }
      }
    }

    override def outputPartitioning: Partitioning = child.outputPartitioning

    override def inputRDDs(): Seq[RDD[InternalRow]] = {
      child.asInstanceOf[CodegenSupport].inputRDDs()
    }

    override def doProduce(ctx: CodegenContext): String = {
      child.asInstanceOf[CodegenSupport].produce(ctx, this)
    }

    override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
      consume(ctx, input)
    }

    override def doExecuteBroadcast[T](): Broadcast[T] = {
      child.executeBroadcast()
    }

    override def doExecuteColumnar(): RDD[ColumnarBatch] = {
      child.executeColumnar()
    }

    override def supportsColumnar: Boolean = child.supportsColumnar

    override protected def withNewChildInternal(newChild: SparkPlan): DebugExec =
      copy(child = newChild)
  }
}
