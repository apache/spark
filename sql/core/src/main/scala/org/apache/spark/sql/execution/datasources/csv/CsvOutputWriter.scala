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
package org.apache.spark.sql.execution.datasources.csv

import java.nio.charset.Charset

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVOptions, UnivocityGenerator}
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType

class CsvOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    params: CSVOptions) extends OutputWriter with Logging {

  private var univocityGenerator: Option[UnivocityGenerator] = None

  if (params.headerFlag) {
    val gen = getGen()
    gen.writeHeaders()
  }

  private def getGen(): UnivocityGenerator = univocityGenerator.getOrElse {
    val charset = Charset.forName(params.charset)
    val os = CodecStreams.createOutputStreamWriter(context, new Path(path), charset)
    val newGen = new UnivocityGenerator(dataSchema, os, params)
    univocityGenerator = Some(newGen)
    newGen
  }

  override def write(row: InternalRow): Unit = {
    val gen = getGen()
    gen.write(row)
  }

  override def close(): Unit = univocityGenerator.foreach(_.close())
}
