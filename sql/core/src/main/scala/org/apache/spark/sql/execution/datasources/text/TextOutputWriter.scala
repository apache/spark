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
package org.apache.spark.sql.execution.datasources.text

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType

class TextOutputWriter(
    val path: String,
    dataSchema: StructType,
    lineSeparator: Array[Byte],
    context: TaskAttemptContext)
  extends OutputWriter {

  private val writer = CodecStreams.createOutputStream(context, new Path(path))

  override def write(row: InternalRow): Unit = {
    if (!row.isNullAt(0)) {
      val utf8string = row.getUTF8String(0)
      utf8string.writeTo(writer)
    }
    writer.write(lineSeparator)
  }

  override def close(): Unit = {
    writer.close()
  }
}
