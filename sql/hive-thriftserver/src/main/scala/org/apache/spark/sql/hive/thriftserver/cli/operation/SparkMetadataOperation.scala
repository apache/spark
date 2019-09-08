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

package org.apache.spark.sql.hive.thriftserver.cli.operation

import java.util

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin._
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.cli.{CLOSED, OperationType}
import org.apache.spark.sql.hive.thriftserver.cli.session.ThriftSession
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException
import org.apache.spark.sql.types.StructType


abstract class SparkMetadataOperation(session: ThriftSession, opType: OperationType)
  extends Operation(session, opType, false) with Logging {

  protected val DEFAULT_HIVE_CATALOG: String = ""
  protected var RESULT_SET_SCHEMA: StructType = null
  protected val SEARCH_STRING_ESCAPE: Char = '\\'

  setHasResultSet(true)

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#close()
   */
  @throws[SparkThriftServerSQLException]
  override def close(): Unit = {
    setState(CLOSED)
    cleanupOperationLog()
  }

  /**
   * Convert wildchars and escape sequence from JDBC format to datanucleous/regex
   */
  protected def convertIdentifierPattern(pattern: String, datanucleusFormat: Boolean): String =
    if (pattern == null) {
      convertPattern("%", true)
    } else {
      convertPattern(pattern, datanucleusFormat)
    }

  /**
   * Convert wildchars and escape sequence of schema pattern from JDBC format to datanucleous/regex
   * The schema pattern treats empty string also as wildchar
   */
  protected def convertSchemaPattern(pattern: String): String =
    if ((pattern == null) || pattern.isEmpty) {
      convertPattern("%", true)
    } else {
      convertPattern(pattern, true)
    }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   *                these characters escaped using { @link #getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped
   *         characters.
   *
   *         The datanucleus module expects the wildchar as '*'. The columns search on the
   *         other hand is done locally inside the hive code and that requires the regex wildchar
   *         format '.*'  This is driven by the datanucleusFormat flag.
   */
  private def convertPattern(pattern: String, datanucleusFormat: Boolean): String = {
    var wStr: String = null
    if (datanucleusFormat) wStr = "*"
    else wStr = ".*"
    pattern.replaceAll("([^\\\\])%", "$1" + wStr)
      .replaceAll("\\\\%", "%").replaceAll("^%", wStr)
      .replaceAll("([^\\\\])_", "$1.")
      .replaceAll("\\\\_", "_")
      .replaceAll("^_", ".")
  }

  protected def isAuthV2Enabled: Boolean = {
    val ss: SessionState = SessionState.get
    ss.isAuthorizationModeV2 && HiveConf.getBoolVar(ss.getConf,
      HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)
  }

  @throws[SparkThriftServerSQLException]
  protected def authorizeMetaGets(opType: HiveOperationType,
                                  inpObjs: util.List[HivePrivilegeObject]): Unit = {
    authorizeMetaGets(opType, inpObjs, null)
  }

  @throws[SparkThriftServerSQLException]
  protected def authorizeMetaGets(opType: HiveOperationType,
                                  inpObjs: util.List[HivePrivilegeObject],
                                  cmdString: String): Unit = {
    val ss: SessionState = SessionState.get
    val ctxBuilder: HiveAuthzContext.Builder = new HiveAuthzContext.Builder
    ctxBuilder.setUserIpAddress(ss.getUserIpAddress)
    ctxBuilder.setCommandString(cmdString)
    try {
      ss.getAuthorizerV2.checkPrivileges(opType, inpObjs, null, ctxBuilder.build)
    } catch {
      case e@(_: HiveAuthzPluginException | _: HiveAccessControlException) =>
        throw new SparkThriftServerSQLException(e.getMessage, e)
    }
  }
}
