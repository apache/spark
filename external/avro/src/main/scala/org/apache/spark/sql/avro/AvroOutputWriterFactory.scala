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

import org.apache.avro.Schema
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

/**
 * A factory that produces [[AvroOutputWriter]].
 * @param catalystSchema Catalyst schema of input data.
 * @param avroSchemaAsJsonString Avro schema of output result, in JSON string format.
 */
private[avro] class AvroOutputWriterFactory(
    catalystSchema: StructType,
    avroSchemaAsJsonString: String) extends OutputWriterFactory {

  private lazy val avroSchema = new Schema.Parser().parse(avroSchemaAsJsonString)

  override def getFileExtension(context: TaskAttemptContext): String = ".avro"

  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    new AvroOutputWriter(path, context, catalystSchema, avroSchema)
  }
}
