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

package org.apache.spark.sql.execution.python

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.storage.{BlockId, PythonWorkerLogBlockId, PythonWorkerLogLine}

case class PythonWorkerLogsExec(jsonAttr: Attribute)
  extends LeafExecNode {

  override def output: Seq[Attribute] = Seq(jsonAttr)

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    import session.implicits._

    val blockIds = getBlockIds(session.sessionUUID)
    val rdd = new BlockRDD[PythonWorkerLogLine](session.sparkContext, blockIds.toArray)

    val encoder = encoderFor[String]
    val toRow = encoder.createSerializer()

    val numOutputRows = longMetric("numOutputRows")
    rdd.mapPartitionsInternal { iter =>
      iter.map { value =>
        numOutputRows += 1
        toRow(value.message).copy()
      }
    }
  }

  private def getBlockIds(sessionId: String): Seq[BlockId] = {
    val blockManager = SparkEnv.get.blockManager.master
    blockManager.getMatchingBlockIds(
      id => id.isInstanceOf[PythonWorkerLogBlockId] &&
        id.asInstanceOf[PythonWorkerLogBlockId].sessionId == sessionId,
      askStorageEndpoints = true
    ).distinct
  }
}
