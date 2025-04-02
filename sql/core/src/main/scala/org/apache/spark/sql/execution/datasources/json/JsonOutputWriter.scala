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
package org.apache.spark.sql.execution.datasources.json

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{ENCODING, PATH}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType

class JsonOutputWriter(
    val path: String,
    options: JSONOptions,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with Logging {

  private val encoding = options.encoding match {
    case Some(charsetName) => Charset.forName(charsetName)
    case None => StandardCharsets.UTF_8
  }

  if (JSONOptionsInRead.denyList.contains(encoding)) {
    logWarning(log"The JSON file (${MDC(PATH, path)}) was written in the encoding " +
      log"${MDC(ENCODING, encoding.displayName())} which can be read back by Spark only " +
      log"if multiLine is enabled.")
  }

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), encoding)

  // create the Generator without separator inserted between 2 records
  private[this] val gen = new JacksonGenerator(dataSchema, writer, options)

  override def write(row: InternalRow): Unit = {
    gen.write(row)
    gen.writeLineEnding()
  }

  override def close(): Unit = {
    gen.close()
    writer.close()
  }
}
