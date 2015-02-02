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
package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.{InterruptibleIterator, Logging, Partition, TaskContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
 * HBaseSQLReaderRDD
 */
class HBaseSQLReaderRDD(
                         relation: HBaseRelation,
                         codegenEnabled: Boolean,
                         output: Seq[Attribute],
                         filterPred: Option[Expression],
                         coprocSubPlan: Option[SparkPlan],
                         @transient sqlContext: SQLContext)
  extends RDD[Row](sqlContext.sparkContext, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    RangeCriticalPoint.generatePrunedPartitions(relation, filterPred).toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map {
      identity
    }.toSeq
  }

  // For critical-point-based predicate pushdown
  // Identical to compute2 with the addition of partition-specific
  // partial reduction for those partitions mapped to multiple critical point ranges,
  // as indicated by the keyPartialEvalIndex in the partition, where the original
  // filter predicate will be used
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition = split.asInstanceOf[HBasePartition]
    val (filters, otherFilters) = relation.buildFilter(output,
      partition.computePredicate(relation))
    val scan = relation.buildScan(split, filters, output)
    scan.setCaching(relation.scannerFetchSize)
    logDebug("Scanner Fetch Size: " + s"${relation.scannerFetchSize}")
    val scanner = relation.htable.getScanner(scan)
    val otherFilter: (Row) => Boolean = if (otherFilters.isDefined) {
      if (codegenEnabled) {
        GeneratePredicate(otherFilters.get, output)
      } else {
        InterpretedPredicate(otherFilters.get, output)
      }
    } else null

    val lBuffer = ListBuffer[HBaseRawType]()
    val aBuffer = ArrayBuffer[Byte]()
    val row = new GenericMutableRow(output.size)
    val projections = output.zipWithIndex

    var finished: Boolean = false
    var gotNext: Boolean = false
    var result: Result = null

    val iter = new Iterator[Row] {
      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            result = scanner.next
            finished = result == null
            gotNext = true
          }
        }
        if (finished) {
          close()
        }
        !finished
      }

      override def next(): Row = {
        if (hasNext) {
          gotNext = false
          relation.buildRow(projections, result, lBuffer, aBuffer, row)
        } else {
          null
        }
      }

      def close() = {
        try {
          scanner.close()
          relation.closeHTable()
        } catch {
          case e: Exception => logWarning("Exception in scanner.close", e)
        }
      }
    }
    if (otherFilter == null) {
      new InterruptibleIterator(context, iter)
    } else {
      new InterruptibleIterator(context, iter.filter(otherFilter))
    }
  }
}
