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
package org.apache.spark.sql.execution.datasources.v2.fwf

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.sql.catalyst.fwf.FixedWidthOptions
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.fwf.FixedWidthOutputWriter
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

case class FixedWidthWrite(
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean,
    info: LogicalWriteInfo) extends FileWrite {

  override def prepareWrite(
      sqlConf: SQLConf,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val parsedOptions = new FixedWidthOptions(options, sqlConf.sessionLocalTimeZone)

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new FixedWidthOutputWriter(path, dataSchema, context, parsedOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = ".txt"
    }
  }
}
