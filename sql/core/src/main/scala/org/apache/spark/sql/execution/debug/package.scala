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
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.sql.catalyst.trees.TreeNodeRef

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
   * Augments SchemaRDDs with debug methods.
   */
  @DeveloperApi
  implicit class DebugQuery(query: SchemaRDD) {
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
     * A collection of stats for each column of output.
     * @param elementTypes the actual runtime types for the output.  Useful when there are bugs
     *        causing the wrong data to be projected.
     */
    case class ColumnStat(
        elementTypes: Accumulator[HashSet[String]] = sparkContext.accumulator(HashSet.empty))
    val tupleCount = sparkContext.accumulator[Int](0)

    val numColumns = child.output.size
    val columnStats = Array.fill(child.output.size)(new ColumnStat())

    def dumpStats(): Unit = {
      println(s"== ${child.simpleString} ==")
      println(s"Tuples output: ${tupleCount.value}")
      child.output.zip(columnStats).foreach { case(attr, stat) =>
        val actualDataTypes =stat.elementTypes.value.mkString("{", ",", "}")
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
}
