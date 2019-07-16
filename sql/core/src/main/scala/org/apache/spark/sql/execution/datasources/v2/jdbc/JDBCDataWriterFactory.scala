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

package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.sql.Connection

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

/* Writer factory that's serialized and send to executors
 */
class JDBCDataWriterFactory(options: JdbcOptionsInWrite, schema: StructType) extends
  DataWriterFactory with Logging{
  def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    // TODO : Check if every task in executor call createWriter to gets its own writer object.
    // JDBC connection should not be shared between Tasks.
    // JDBCUtil.createConnectionFactory should take care of that??
    logInfo(s"***dsv2-flows*** createWriter called for partition $partitionId taskID $taskId")
    val conn: Connection = JdbcUtils.createConnectionFactory(options)()
    new JDBCDataWriter(options, conn, schema)
  }
}
