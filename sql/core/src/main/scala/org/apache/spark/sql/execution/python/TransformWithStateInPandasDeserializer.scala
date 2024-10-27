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

import java.io.DataInputStream

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

/**
 * A helper class to deserialize state Arrow batches from the state socket in
 * TransformWithStateInPandas.
 */
class TransformWithStateInPandasDeserializer(deserializer: ExpressionEncoder.Deserializer[Row])
  extends Logging {
  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader for transformWithStateInPandas state socket", 0, Long.MaxValue)

  /**
   * Read Arrow batches from the given stream and deserialize them into rows.
   */
  def readArrowBatches(stream: DataInputStream): Seq[Row] = {
    val reader = new ArrowStreamReader(stream, allocator)
    val root = reader.getVectorSchemaRoot
    val vectors = root.getFieldVectors.asScala.map { vector =>
      new ArrowColumnVector(vector)
    }.toArray[ColumnVector]
    val rows = ArrayBuffer[Row]()
    while (reader.loadNextBatch()) {
      val batch = new ColumnarBatch(vectors)
      batch.setNumRows(root.getRowCount)
      rows.appendAll(batch.rowIterator().asScala.map(r => deserializer(r.copy())))
    }
    reader.close(false)
    rows.toSeq
  }
}
