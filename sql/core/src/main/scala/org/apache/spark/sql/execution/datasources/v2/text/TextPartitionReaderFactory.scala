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
package org.apache.spark.sql.execution.datasources.v2.text

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, HadoopFileWholeTextReader, PartitionedFile}
import org.apache.spark.sql.execution.datasources.text.TextOptions
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A factory used to create Text readers.
 *
 * @param sqlConf SQL configuration.
 * @param broadcastedConf Broadcasted serializable Hadoop Configuration.
 * @param readDataSchema Required schema in the batch scan.
 * @param partitionSchema Schema of partitions.
 * @param options Options for reading a text file.
 * */
case class TextPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: TextOptions) extends FilePartitionReaderFactory {

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val confValue = broadcastedConf.value.value
    val reader = Utils.createResourceUninterruptiblyIfInTaskThread {
      if (!options.wholeText) {
        new HadoopFileLinesReader(file, options.lineSeparatorInRead, confValue)
      } else {
        new HadoopFileWholeTextReader(file, confValue)
      }
    }
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => reader.close()))
    val iter = if (readDataSchema.isEmpty) {
      val emptyUnsafeRow = new UnsafeRow(0)
      reader.map(_ => emptyUnsafeRow)
    } else {
      val unsafeRowWriter = new UnsafeRowWriter(1)

      reader.map { line =>
        // Writes to an UnsafeRow directly
        unsafeRowWriter.reset()
        unsafeRowWriter.write(0, line.getBytes, 0, line.getLength)
        unsafeRowWriter.getRow()
      }
    }
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }
}
