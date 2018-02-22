/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli;

/**
 * OperationStatus
 *
 */
public class OperationStatus {

  private final OperationState state;
  private final String taskStatus;
  private final long operationStarted;
  private final long operationCompleted;
  private final boolean hasResultSet;
  private final HiveSQLException operationException;

  public OperationStatus(OperationState state, String taskStatus, long operationStarted,
      long operationCompleted, boolean hasResultSet, HiveSQLException operationException) {
    this.state = state;
    this.taskStatus = taskStatus;
    this.operationStarted = operationStarted;
    this.operationCompleted = operationCompleted;
    this.hasResultSet = hasResultSet;
    this.operationException = operationException;
  }

  public OperationState getState() {
    return state;
  }

  public String getTaskStatus() {
    return taskStatus;
  }

  public long getOperationStarted() {
    return operationStarted;
  }

  public long getOperationCompleted() {
    return operationCompleted;
  }

  public boolean getHasResultSet() {
    return hasResultSet;
  }

  public HiveSQLException getOperationException() {
    return operationException;
  }

}
