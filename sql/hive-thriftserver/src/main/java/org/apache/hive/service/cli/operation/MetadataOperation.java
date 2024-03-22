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
