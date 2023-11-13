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

package org.apache.hive.service.cli.operation;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * MetadataOperation.
 *
 */
public abstract class MetadataOperation extends Operation {

  protected static final String DEFAULT_HIVE_CATALOG = "";
  protected static TableSchema RESULT_SET_SCHEMA;
  private static final char SEARCH_STRING_ESCAPE = '\\';

  protected MetadataOperation(HiveSession parentSession, OperationType opType) {
    super(parentSession, opType);
    setHasResultSet(true);
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#close()
   */
  @Override
  public void close() throws HiveSQLException {
    setState(OperationState.CLOSED);
    cleanupOperationLog();
  }

  /**
   * Convert wildchars and escape sequence from JDBC format to datanucleous/regex
   */
  protected String convertIdentifierPattern(final String pattern, boolean datanucleusFormat) {
    if (pattern == null) {
      return convertPattern("%", true);
    } else {
      return convertPattern(pattern, datanucleusFormat);
    }
  }

  /**
   * Convert wildchars and escape sequence of schema pattern from JDBC format to datanucleous/regex
   * The schema pattern treats empty string also as wildchar
   */
  protected String convertSchemaPattern(final String pattern) {
    if ((pattern == null) || pattern.isEmpty()) {
      return convertPattern("%", true);
    } else {
      return convertPattern(pattern, true);
    }
  }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   * these characters escaped using {@link #getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped
   * characters.
   *
   * The datanucleus module expects the wildchar as '*'. The columns search on the
   * other hand is done locally inside the hive code and that requires the regex wildchar
   * format '.*'  This is driven by the datanucleusFormat flag.
   */
  private String convertPattern(final String pattern, boolean datanucleusFormat) {
    String wStr;
    if (datanucleusFormat) {
      wStr = "*";
    } else {
      wStr = ".*";
    }
    return pattern
        .replaceAll("([^\\\\])%", "$1" + wStr).replaceAll("\\\\%", "%").replaceAll("^%", wStr)
        .replaceAll("([^\\\\])_", "$1.").replaceAll("\\\\_", "_").replaceAll("^_", ".");
  }

  protected boolean isAuthV2Enabled(){
    SessionState ss = SessionState.get();
    return (ss.isAuthorizationModeV2() &&
        HiveConf.getBoolVar(ss.getConf(), HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED));
  }

  protected void authorizeMetaGets(HiveOperationType opType, List<HivePrivilegeObject> inpObjs)
      throws HiveSQLException {
    authorizeMetaGets(opType, inpObjs, null);
  }

  protected void authorizeMetaGets(HiveOperationType opType, List<HivePrivilegeObject> inpObjs,
      String cmdString) throws HiveSQLException {
    SessionState ss = SessionState.get();
    HiveAuthzContext.Builder ctxBuilder = new HiveAuthzContext.Builder();
    ctxBuilder.setUserIpAddress(ss.getUserIpAddress());
    ctxBuilder.setCommandString(cmdString);
    try {
      ss.getAuthorizerV2().checkPrivileges(opType, inpObjs, null,
          ctxBuilder.build());
    } catch (HiveAuthzPluginException | HiveAccessControlException e) {
      throw new HiveSQLException(e.getMessage(), e);
    }
  }

}
