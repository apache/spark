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

package org.apache.spark.sql.hive.thriftserver

import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.thrift.Type
import org.apache.hive.service.cli.{RowSet, RowSetFactory, TableSchema}
import org.slf4j.LoggerFactory

/**
 * Various utilities for hive-thriftserver used to upgrade the built-in Hive.
 */
private[thriftserver] object ThriftserverShimUtils {

  private[thriftserver] type TProtocolVersion = org.apache.hive.service.rpc.thrift.TProtocolVersion
  private[thriftserver] type Client = org.apache.hive.service.rpc.thrift.TCLIService.Client
  private[thriftserver] type TOpenSessionReq = org.apache.hive.service.rpc.thrift.TOpenSessionReq
  private[thriftserver] type TGetSchemasReq = org.apache.hive.service.rpc.thrift.TGetSchemasReq
  private[thriftserver] type TGetTablesReq = org.apache.hive.service.rpc.thrift.TGetTablesReq
  private[thriftserver] type TGetColumnsReq = org.apache.hive.service.rpc.thrift.TGetColumnsReq

  private[thriftserver] def getConsole: SessionState.LogHelper = {
    val LOG = LoggerFactory.getLogger(classOf[SparkSQLCLIDriver])
    new SessionState.LogHelper(LOG)
  }

  private[thriftserver] def resultRowSet(
      getResultSetSchema: TableSchema,
      getProtocolVersion: TProtocolVersion): RowSet = {
    RowSetFactory.create(getResultSetSchema, getProtocolVersion, false)
  }

  private[thriftserver] def toJavaSQLType(s: String): Int = Type.getType(s).toJavaSQLType

}
