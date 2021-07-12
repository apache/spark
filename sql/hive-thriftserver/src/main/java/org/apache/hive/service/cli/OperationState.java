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

package org.apache.hive.service.cli;

import org.apache.hive.service.rpc.thrift.TOperationState;

/**
 * OperationState.
 *
 */
public enum OperationState {
  INITIALIZED(TOperationState.INITIALIZED_STATE, false),
  RUNNING(TOperationState.RUNNING_STATE, false),
  FINISHED(TOperationState.FINISHED_STATE, true),
  CANCELED(TOperationState.CANCELED_STATE, true),
  CLOSED(TOperationState.CLOSED_STATE, true),
  ERROR(TOperationState.ERROR_STATE, true),
  UNKNOWN(TOperationState.UKNOWN_STATE, false),
  PENDING(TOperationState.PENDING_STATE, false),
  TIMEDOUT(TOperationState.TIMEDOUT_STATE, true);

  private final TOperationState tOperationState;
  private final boolean terminal;

  OperationState(TOperationState tOperationState, boolean terminal) {
    this.tOperationState = tOperationState;
    this.terminal = terminal;
  }

  // must be sync with TOperationState in order
  public static OperationState getOperationState(TOperationState tOperationState) {
    return OperationState.values()[tOperationState.getValue()];
  }

  public static void validateTransition(OperationState oldState,
      OperationState newState)
          throws HiveSQLException {
    switch (oldState) {
    case INITIALIZED:
      switch (newState) {
      case PENDING:
      case RUNNING:
      case CANCELED:
      case CLOSED:
      case TIMEDOUT:
        return;
      }
      break;
    case PENDING:
      switch (newState) {
      case RUNNING:
      case FINISHED:
      case CANCELED:
      case ERROR:
      case CLOSED:
      case TIMEDOUT:
        return;
      }
      break;
    case RUNNING:
      switch (newState) {
      case FINISHED:
      case CANCELED:
      case ERROR:
      case CLOSED:
      case TIMEDOUT:
        return;
      }
      break;
    case FINISHED:
    case CANCELED:
    case TIMEDOUT:
    case ERROR:
      if (OperationState.CLOSED.equals(newState)) {
        return;
      }
      break;
    default:
      // fall-through
    }
    throw new HiveSQLException("Illegal Operation state transition " +
        "from " + oldState + " to " + newState);
  }

  public void validateTransition(OperationState newState)
      throws HiveSQLException {
    validateTransition(this, newState);
  }

  public TOperationState toTOperationState() {
    return tOperationState;
  }

  public boolean isTerminal() {
    return terminal;
  }
}
