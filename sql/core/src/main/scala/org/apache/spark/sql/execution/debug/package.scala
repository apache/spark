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

import scala.collection.mutable.HashSet

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.internal.SQLConf
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
 */
package object debug {

  /** Helper function to evade the println() linter. */
  private def debugPrint(msg: String): Unit = {
    // scalastyle:off println
    println(msg)
    // scalastyle:on println
  }

  def codegenString(plan: SparkPlan): String = {
    val codegenSubtrees = new collection.mutable.HashSet[WholeStageCodegenExec]()
    plan transform {
      case s: WholeStageCodegenExec =>
        codegenSubtrees += s
        s
      case s => s
    }
    var output = s"Found ${codegenSubtrees.size} WholeStageCodegen subtrees.\n"
    for ((s, i) <- codegenSubtrees.toSeq.zipWithIndex) {
      output += s"== Subtree ${i + 1} / ${codegenSubtrees.size} ==\n"
      output += s
      output += "\nGenerated code:\n"
      val (_, source) = s.doCodeGen()
      output += s"${CodeFormatter.format(source)}\n"
    }
    output
  }

  /**
   * Augments [[SparkSession]] with debug methods.
   */
  implicit class DebugSQLContext(sparkSession: SparkSession) {
    def debug(): Unit = {
      sparkSession.conf.set(SQLConf.DATAFRAME_EAGER_ANALYSIS.key, false)
    }
  }

  /**
   * Augments [[Dataset]]s with debug methods.
   */
  implicit class DebugQuery(query: Dataset[_]) extends Logging {
    def debug(): Unit = {
      val plan = query.queryExecution.executedPlan
      val visited = new collection.mutable.HashSet[TreeNodeRef]()
      val debugPlan = plan transform {
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

  private[sql] case class DebugExec(child: SparkPlan) extends UnaryExecNode with CodegenSupport {
    def output: Seq[Attribute] = child.output

    class SetAccumulator[T] extends AccumulatorV2[T, HashSet[T]] {
      private val _set = new HashSet[T]()
      override def isZero: Boolean = _set.isEmpty
      override def copy(): AccumulatorV2[T, HashSet[T]] = {
        val newAcc = new SetAccumulator[T]()
        newAcc._set ++= _set
        newAcc
      }
      override def reset(): Unit = _set.clear()
      override def add(v: T): Unit = _set += v
      override def merge(other: AccumulatorV2[T, HashSet[T]]): Unit = _set ++= other.value
      override def value: HashSet[T] = _set
    }

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
      debugPrint(s"== ${child.simpleString} ==")
      debugPrint(s"Tuples output: ${tupleCount.value}")
      child.output.zip(columnStats).foreach { case (attr, metric) =>
        val actualDataTypes = metric.elementTypes.value.mkString("{", ",", "}")
        debugPrint(s" ${attr.name} ${attr.dataType}: $actualDataTypes")
      }
    }

    protected override def doExecute(): RDD[InternalRow] = {
      child.execute().mapPartitions { iter =>
        new Iterator[InternalRow] {
          def hasNext: Boolean = iter.hasNext

          def next(): InternalRow = {
            val currentRow = iter.next()
            tupleCount.add(1)
            var i = 0
            while (i < numColumns) {
              val value = currentRow.get(i, output(i).dataType)
              if (value != null) {
                columnStats(i).elementTypes.add(value.getClass.getName)
              }
              i += 1
            }
            currentRow
          }
        }
      }
    }

    override def inputRDDs(): Seq[RDD[InternalRow]] = {
      child.asInstanceOf[CodegenSupport].inputRDDs()
    }

    override def doProduce(ctx: CodegenContext): String = {
      child.asInstanceOf[CodegenSupport].produce(ctx, this)
    }

    override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
      consume(ctx, input)
    }
  }
}
