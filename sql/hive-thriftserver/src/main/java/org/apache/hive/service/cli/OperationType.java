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

import org.apache.hive.service.rpc.thrift.TOperationType;

/**
 * OperationType.
 *
 */
public enum OperationType {

  UNKNOWN_OPERATION(TOperationType.UNKNOWN),
  EXECUTE_STATEMENT(TOperationType.EXECUTE_STATEMENT),
  PROCEDURAL_SQL(TOperationType.PROCEDURAL_SQL),
  GET_TYPE_INFO(TOperationType.GET_TYPE_INFO),
  GET_CATALOGS(TOperationType.GET_CATALOGS),
  GET_SCHEMAS(TOperationType.GET_SCHEMAS),
  GET_TABLES(TOperationType.GET_TABLES),
  GET_TABLE_TYPES(TOperationType.GET_TABLE_TYPES),
  GET_COLUMNS(TOperationType.GET_COLUMNS),
  GET_FUNCTIONS(TOperationType.GET_FUNCTIONS);

  private TOperationType tOperationType;

  OperationType(TOperationType tOpType) {
    this.tOperationType = tOpType;
  }

  public static OperationType getOperationType(TOperationType tOperationType) {
    // TODO: replace this with a Map?
    for (OperationType opType : values()) {
      if (tOperationType.equals(opType.tOperationType)) {
        return opType;
      }
    }
    return OperationType.UNKNOWN_OPERATION;
  }

  public TOperationType toTOperationType() {
    return tOperationType;
  }
}
