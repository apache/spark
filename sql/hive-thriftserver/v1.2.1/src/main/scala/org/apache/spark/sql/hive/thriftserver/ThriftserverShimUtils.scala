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

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.{RowSet, RowSetFactory, TableSchema, Type}

/**
 * Various utilities for hive-thriftserver used to upgrade the built-in Hive.
 */
private[thriftserver] object ThriftserverShimUtils {

  private[thriftserver] type TProtocolVersion = org.apache.hive.service.cli.thrift.TProtocolVersion
  private[thriftserver] type Client = org.apache.hive.service.cli.thrift.TCLIService.Client
  private[thriftserver] type TOpenSessionReq = org.apache.hive.service.cli.thrift.TOpenSessionReq
  private[thriftserver] type TGetSchemasReq = org.apache.hive.service.cli.thrift.TGetSchemasReq
  private[thriftserver] type TGetTablesReq = org.apache.hive.service.cli.thrift.TGetTablesReq
  private[thriftserver] type TGetColumnsReq = org.apache.hive.service.cli.thrift.TGetColumnsReq

  private[thriftserver] def getConsole: SessionState.LogHelper = {
    val LOG = LogFactory.getLog(classOf[SparkSQLCLIDriver])
    new SessionState.LogHelper(LOG)
  }

  private[thriftserver] def resultRowSet(
      getResultSetSchema: TableSchema,
      getProtocolVersion: TProtocolVersion): RowSet = {
    RowSetFactory.create(getResultSetSchema, getProtocolVersion)
  }

  private[thriftserver] def toJavaSQLType(s: String): Int = Type.getType(s).toJavaSQLType

}
