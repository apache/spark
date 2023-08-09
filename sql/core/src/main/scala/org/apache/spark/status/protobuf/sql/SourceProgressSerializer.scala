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

package org.apache.spark.status.protobuf.sql

import java.util.{HashMap => JHashMap, List => JList}

import org.apache.spark.sql.streaming.SourceProgress
import org.apache.spark.status.protobuf.StoreTypes
import org.apache.spark.status.protobuf.Utils.{getStringField, setJMapField, setStringField}

private[protobuf] object SourceProgressSerializer {

  def serialize(source: SourceProgress): StoreTypes.SourceProgress = {
    val builder = StoreTypes.SourceProgress.newBuilder()
    setStringField(source.description, builder.setDescription)
    setStringField(source.startOffset, builder.setStartOffset)
    setStringField(source.endOffset, builder.setEndOffset)
    setStringField(source.latestOffset, builder.setLatestOffset)
    builder.setNumInputRows(source.numInputRows)
    builder.setInputRowsPerSecond(source.inputRowsPerSecond)
    builder.setProcessedRowsPerSecond(source.processedRowsPerSecond)
    setJMapField(source.metrics, builder.putAllMetrics)
    builder.build()
  }

  def deserializeToArray(sourceList: JList[StoreTypes.SourceProgress]): Array[SourceProgress] = {
    val size = sourceList.size()
    val result = new Array[SourceProgress](size)
    var i = 0
    while (i < size) {
      result(i) = deserialize(sourceList.get(i))
      i += 1
    }
    result
  }

  private def deserialize(source: StoreTypes.SourceProgress): SourceProgress = {
    new SourceProgress(
      description = getStringField(source.hasDescription, () => source.getDescription),
      startOffset = getStringField(source.hasStartOffset, () => source.getStartOffset),
      endOffset = getStringField(source.hasEndOffset, () => source.getEndOffset),
      latestOffset = getStringField(source.hasLatestOffset, () => source.getLatestOffset),
      numInputRows = source.getNumInputRows,
      inputRowsPerSecond = source.getInputRowsPerSecond,
      processedRowsPerSecond = source.getProcessedRowsPerSecond,
      metrics = new JHashMap(source.getMetricsMap)
    )
  }
}
