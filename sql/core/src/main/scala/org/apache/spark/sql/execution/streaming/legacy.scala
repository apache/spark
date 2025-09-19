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

package org.apache.spark.sql.execution.streaming

import scala.reflect.ClassTag

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.checkpointing.{HDFSMetadataLog => ActualHDFSMetadataLog}
import org.apache.spark.sql.execution.streaming.runtime.{SerializedOffset => ActualSerializedOffset}

@deprecated("use org.apache.spark.sql.execution.streaming.checkpointing.HDFSMetadataLog")
class HDFSMetadataLog[T <: AnyRef: ClassTag](sparkSession: SparkSession, path: String)
  extends ActualHDFSMetadataLog[T](sparkSession, path)

@deprecated("use org.apache.spark.sql.execution.streaming.runtime.SerializedOffset")
object SerializedOffset {
  def apply(offset: String): ActualSerializedOffset = ActualSerializedOffset(offset)
}
