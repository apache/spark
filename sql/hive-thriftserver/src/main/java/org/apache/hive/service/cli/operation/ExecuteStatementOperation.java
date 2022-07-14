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

import java.util.Map;

import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.session.HiveSession;

public abstract class ExecuteStatementOperation extends Operation {
  protected String statement = null;

  public ExecuteStatementOperation(HiveSession parentSession, String statement,
      Map<String, String> confOverlay, boolean runInBackground) {
    super(parentSession, confOverlay, OperationType.EXECUTE_STATEMENT, runInBackground);
    this.statement = statement;
  }

  public String getStatement() {
    return statement;
  }

  protected void registerCurrentOperationLog() {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        LOG.warn("Failed to get current OperationLog object of Operation: " +
          getHandle().getHandleIdentifier());
        isOperationLogEnabled = false;
        return;
      }
      OperationLog.setCurrentOperationLog(operationLog);
    }
  }
}
