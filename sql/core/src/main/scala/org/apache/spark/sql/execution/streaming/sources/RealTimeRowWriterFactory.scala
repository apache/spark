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

package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.SparkEnv
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.util.RpcUtils

/**
 * A [[StreamingDataWriterFactory]] that creates [[RealTimeRowWriter]], which sends rows to
 * the driver in real-time through RPC.
 *
 * Note that, because it sends all rows to the driver, this factory will generally be unsuitable
 * for production-quality sinks. It's intended for use in tests.
 *
 */
case class RealTimeRowWriterFactory(
    driverEndpointName: String,
    driverEndpointAddr: RpcAddress
) extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new RealTimeRowWriter(
      driverEndpointName,
      driverEndpointAddr
    )
  }
}

/**
 * A [[DataWriter]] that sends arrays of rows to the driver in real-time through RPC.
 */
class RealTimeRowWriter(
    driverEndpointName: String,
    driverEndpointAddr: RpcAddress
) extends DataWriter[InternalRow] {

  private val endpointRef = RpcUtils.makeDriverRef(
    driverEndpointName,
    driverEndpointAddr.host,
    driverEndpointAddr.port,
    SparkEnv.get.rpcEnv
  )

  // Spark reuses the same `InternalRow` instance, here we copy it before buffer it.
  override def write(row: InternalRow): Unit = {
    endpointRef.send(Array(row.copy()))
  }

  override def commit(): WriterCommitMessage = { null }

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
