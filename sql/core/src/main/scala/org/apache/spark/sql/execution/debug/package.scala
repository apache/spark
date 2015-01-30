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

import org.apache.spark.{AccumulatorParam, Accumulator, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.types._

/**
 * :: DeveloperApi ::
 * Contains methods for debugging query execution.
 *
 * Usage:
 * {{{
 *   sql("SELECT key FROM src").debug
 * }}}
 */
package object debug {

  /**
   * :: DeveloperApi ::
   * Augments [[DataFrame]]s with debug methods.
   */
  @DeveloperApi
  implicit class DebugQuery(query: DataFrame) {
    def debug(): Unit = {
      val plan = query.queryExecution.executedPlan
      val visited = new collection.mutable.HashSet[TreeNodeRef]()
      val debugPlan = plan transform {
        case s: SparkPlan if !visited.contains(new TreeNodeRef(s)) =>
          visited += new TreeNodeRef(s)
          DebugNode(s)
      }
      println(s"Results returned: ${debugPlan.execute().count()}")
      debugPlan.foreach {
        case d: DebugNode => d.dumpStats()
        case _ =>
      }
    }

    def typeCheck(): Unit = {
      val plan = query.queryExecution.executedPlan
      val visited = new collection.mutable.HashSet[TreeNodeRef]()
      val debugPlan = plan transform {
        case s: SparkPlan if !visited.contains(new TreeNodeRef(s)) =>
          visited += new TreeNodeRef(s)
          TypeCheck(s)
      }
      try {
        println(s"Results returned: ${debugPlan.execute().count()}")
      } catch {
        case e: Exception =>
          def unwrap(e: Throwable): Throwable = if (e.getCause == null) e else unwrap(e.getCause)
          println(s"Deepest Error: ${unwrap(e)}")
      }
    }
  }

  private[sql] case class DebugNode(child: SparkPlan) extends UnaryNode {
    def output = child.output

    implicit object SetAccumulatorParam extends AccumulatorParam[HashSet[String]] {
      def zero(initialValue: HashSet[String]): HashSet[String] = {
        initialValue.clear()
        initialValue
      }

      def addInPlace(v1: HashSet[String], v2: HashSet[String]): HashSet[String] = {
        v1 ++= v2
        v1
      }
    }

    /**
     * A collection of metrics for each column of output.
     * @param elementTypes the actual runtime types for the output.  Useful when there are bugs
     *        causing the wrong data to be projected.
     */
    case class ColumnMetrics(
        elementTypes: Accumulator[HashSet[String]] = sparkContext.accumulator(HashSet.empty))
    val tupleCount = sparkContext.accumulator[Int](0)

    val numColumns = child.output.size
    val columnStats = Array.fill(child.output.size)(new ColumnMetrics())

    def dumpStats(): Unit = {
      println(s"== ${child.simpleString} ==")
      println(s"Tuples output: ${tupleCount.value}")
      child.output.zip(columnStats).foreach { case(attr, metric) =>
        val actualDataTypes = metric.elementTypes.value.mkString("{", ",", "}")
        println(s" ${attr.name} ${attr.dataType}: $actualDataTypes")
      }
    }

    def execute() = {
      child.execute().mapPartitions { iter =>
        new Iterator[Row] {
          def hasNext = iter.hasNext
          def next() = {
            val currentRow = iter.next()
            tupleCount += 1
            var i = 0
            while (i < numColumns) {
              val value = currentRow(i)
              if (value != null) {
                columnStats(i).elementTypes += HashSet(value.getClass.getName)
              }
              i += 1
            }
            currentRow
          }
        }
      }
    }
  }

  /**
   * :: DeveloperApi ::
   * Helper functions for checking that runtime types match a given schema.
   */
  @DeveloperApi
  object TypeCheck {
    def typeCheck(data: Any, schema: DataType): Unit = (data, schema) match {
      case (null, _) =>

      case (row: Row, StructType(fields)) =>
        row.toSeq.zip(fields.map(_.dataType)).foreach { case(d, t) => typeCheck(d, t) }
      case (s: Seq[_], ArrayType(elemType, _)) =>
        s.foreach(typeCheck(_, elemType))
      case (m: Map[_, _], MapType(keyType, valueType, _)) =>
        m.keys.foreach(typeCheck(_, keyType))
        m.values.foreach(typeCheck(_, valueType))

      case (_: Long, LongType) =>
      case (_: Int, IntegerType) =>
      case (_: String, StringType) =>
      case (_: Float, FloatType) =>
      case (_: Byte, ByteType) =>
      case (_: Short, ShortType) =>
      case (_: Boolean, BooleanType) =>
      case (_: Double, DoubleType) =>

      case (d, t) => sys.error(s"Invalid data found: got $d (${d.getClass}) expected $t")
    }
  }

  /**
   * :: DeveloperApi ::
   * Augments [[DataFrame]]s with debug methods.
   */
  @DeveloperApi
  private[sql] case class TypeCheck(child: SparkPlan) extends SparkPlan {
    import TypeCheck._

    override def nodeName  = ""

    /* Only required when defining this class in a REPL.
    override def makeCopy(args: Array[Object]): this.type =
      TypeCheck(args(0).asInstanceOf[SparkPlan]).asInstanceOf[this.type]
    */

    def output = child.output

    def children = child :: Nil

    def execute() = {
      child.execute().map { row =>
        try typeCheck(row, child.schema) catch {
          case e: Exception =>
            sys.error(
              s"""
                  |ERROR WHEN TYPE CHECKING QUERY
                  |==============================
                  |$e
                  |======== BAD TREE ============
                  |$child
             """.stripMargin)
        }
        row
      }
    }
  }
}
