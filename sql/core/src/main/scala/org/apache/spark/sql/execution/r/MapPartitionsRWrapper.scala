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

package org.apache.spark.sql.execution.r

import org.apache.spark.api.r._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.api.r.SQLUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

/**
 * A function wrapper that applies the given R function to each partition.
 */
private[sql] case class MapPartitionsRWrapper(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    outputSchema: StructType) extends (Iterator[Any] => Iterator[Any]) {
  def apply(iter: Iterator[Any]): Iterator[Any] = {
    // If the content of current DataFrame is serialized R data?
    val isSerializedRData =
      if (inputSchema == SERIALIZED_R_DATA_SCHEMA) true else false

    val (newIter, deserializer, colNames) =
      if (!isSerializedRData) {
        // Serialize each row into a byte array that can be deserialized in the R worker
        (iter.asInstanceOf[Iterator[Row]].map {row => rowToRBytes(row)},
         SerializationFormats.ROW, inputSchema.fieldNames)
      } else {
        (iter.asInstanceOf[Iterator[Row]].map { row => row(0) }, SerializationFormats.BYTE, null)
      }

    val serializer = if (outputSchema != SERIALIZED_R_DATA_SCHEMA) {
      SerializationFormats.ROW
    } else {
      SerializationFormats.BYTE
    }

    val runner = new RRunner[Array[Byte]](
      func, deserializer, serializer, packageNames, broadcastVars,
      isDataFrame = true, colNames = colNames, mode = RRunnerModes.DATAFRAME_DAPPLY)
    // Partition index is ignored. Dataset has no support for mapPartitionsWithIndex.
    val outputIter = runner.compute(newIter, -1)

    if (serializer == SerializationFormats.ROW) {
      outputIter.map { bytes => bytesToRow(bytes, outputSchema) }
    } else {
      outputIter.map { bytes => Row.fromSeq(Seq(bytes)) }
    }
  }
}
