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

package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.hbase.client.Put
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.execution.{LeafNode, SparkPlan, UnaryNode}
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.util.{DataTypeUtils, HBaseKVHelper}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * :: DeveloperApi ::
 * The HBase table scan operator.
 */
@DeveloperApi
case class HBaseSQLTableScan(
                              relation: HBaseRelation,
                              output: Seq[Attribute],
                              result: RDD[Row]) extends LeafNode {
  override def outputPartitioning = {
    var ordering = List[SortOrder]()
    for (key <- relation.partitionKeys) {
      ordering = ordering :+ SortOrder(key, Ascending)
    }
    RangePartitioning(ordering.toSeq, relation.partitions.size)
  }

  override def execute() = result
}

@DeveloperApi
case class InsertIntoHBaseTable(
                                 relation: HBaseRelation,
                                 child: SparkPlan)
  extends UnaryNode {

  override def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)
    saveAsHbaseFile(childRdd)
    childRdd
  }

  override def output = child.output

  private def saveAsHbaseFile(rdd: RDD[Row]): Unit = {
    // TODO:make the BatchMaxSize configurable
    val BatchMaxSize = 100

    relation.context.sparkContext.runJob(rdd, writeToHbase _)

    def writeToHbase(context: TaskContext, iterator: Iterator[Row]) = {
      val htable = relation.htable
      var rowIndexInBatch = 0
      var colIndexInBatch = 0

      var puts = new ListBuffer[Put]()
      while (iterator.hasNext) {
        val row = iterator.next()
        val rawKeyCol = relation.keyColumns.map(
          kc => {
            val rowColumn = DataTypeUtils.getRowColumnInHBaseRawType(
              row, kc.ordinal, kc.dataType)
            colIndexInBatch += 1
            (rowColumn, kc.dataType)
          }
        )
        val key = HBaseKVHelper.encodingRawKeyColumns(rawKeyCol)
        val put = new Put(key)
        relation.nonKeyColumns.foreach(
          nkc => {
            val rowVal = DataTypeUtils.getRowColumnInHBaseRawType(
              row, nkc.ordinal, nkc.dataType)
            colIndexInBatch += 1
            put.add(nkc.familyRaw, nkc.qualifierRaw, rowVal)
          }
        )

        puts += put
        colIndexInBatch = 0
        rowIndexInBatch += 1
        if (rowIndexInBatch >= BatchMaxSize) {
          htable.put(puts.toList)
          puts.clear()
          rowIndexInBatch = 0
        }
      }
      if (puts.nonEmpty) {
        htable.put(puts.toList)
      }
      relation.closeHTable()
    }
  }
}
