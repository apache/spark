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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.SparkPlan

case class StateStoreRestore(
    keyExpressions: Seq[Attribute],
    checkpointLocation: String,
    operatorId: Long,
    batchId: Long,
    child: SparkPlan) extends execution.UnaryNode {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithStateStore(
      checkpointLocation,
      operatorId = operatorId,
      storeVersion = batchId,
      keyExpressions.toStructType,
      child.output.toStructType,
      new StateStoreConf(sqlContext.conf),
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        var lastRow: Option[UnsafeRow] = None
        val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
        val data = iter.toArray
        val result = data.flatMap { row =>
          val key = getKey(row)
          store.update(key, (f: Option[UnsafeRow]) => {
            lastRow = f
            f.orNull
          })
          row +: lastRow.toSeq
        }

      result.toIterator
    }
  }
  override def output: Seq[Attribute] = child.output
}

case class StateStoreSave(
    keyExpressions: Seq[Attribute],
    checkpointLocation: String,
    operatorId: Long,
    batchId: Long,
    child: SparkPlan) extends execution.UnaryNode {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithStateStore(
      checkpointLocation,
      operatorId = operatorId,
      storeVersion = batchId,
      keyExpressions.toStructType,
      child.output.toStructType,
      new StateStoreConf(sqlContext.conf),
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
      val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)

      val data = iter.toArray

      val result = data.map { row =>
        val key = getKey(row)
        store.update(key.copy(), (_: Option[UnsafeRow]) => {
          row.asInstanceOf[UnsafeRow].copy()
        })
        row
      }

      store.commit()
      store.iterator().map(_._2)
    }
  }

  override def output: Seq[Attribute] = child.output
}