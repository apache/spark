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

package org.apache.spark.status.protobuf.sql

import org.apache.spark.sql.execution.ui.{SQLExecutionDetails, SQLExecutionSummary}
import org.apache.spark.status.protobuf.ProtobufSerDe

private[protobuf] class SQLExecutionSummarySerializer extends ProtobufSerDe[SQLExecutionSummary] {
  private val delegate = new SQLExecutionUIDataSerializer

  override def serialize(value: SQLExecutionSummary): Array[Byte] = delegate.serialize(value.info)

  override def deserialize(bytes: Array[Byte]): SQLExecutionSummary = {
    new SQLExecutionSummary(delegate.deserialize(bytes))
  }
}

private[protobuf] class SQLExecutionDetailsSerializer extends ProtobufSerDe[SQLExecutionDetails] {
  private val delegate = new SQLExecutionUIDataSerializer

  override def serialize(value: SQLExecutionDetails): Array[Byte] = delegate.serialize(value.info)

  override def deserialize(bytes: Array[Byte]): SQLExecutionDetails = {
    new SQLExecutionDetails(delegate.deserialize(bytes))
  }
}
