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

package org.apache.spark.sql.avro

import java.io.{IOException, OutputStream}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[avro] class AvroOutputWriter(
    path: String,
    context: TaskAttemptContext,
    schema: StructType,
    avroSchema: Schema) extends OutputWriter {

  // The input rows will never be null.
  private lazy val serializer = new AvroSerializer(schema, avroSchema, nullable = false)

  /**
   * Overrides the couple of methods responsible for generating the output streams / files so
   * that the data can be correctly partitioned
   */
  private val recordWriter: RecordWriter[AvroKey[GenericRecord], NullWritable] =
    new AvroKeyOutputFormat[GenericRecord]() {

      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }

      @throws(classOf[IOException])
      override def getAvroFileOutputStream(c: TaskAttemptContext): OutputStream = {
        val path = getDefaultWorkFile(context, ".avro")
        path.getFileSystem(context.getConfiguration).create(path)
      }

    }.getRecordWriter(context)

  override def write(row: InternalRow): Unit = {
    val key = new AvroKey(serializer.serialize(row).asInstanceOf[GenericRecord])
    recordWriter.write(key, NullWritable.get())
  }

  override def close(): Unit = recordWriter.close(context)
}
