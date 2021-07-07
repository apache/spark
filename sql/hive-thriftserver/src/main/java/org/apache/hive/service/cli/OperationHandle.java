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

import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;

public class OperationHandle extends Handle {

  private final OperationType opType;
  private final TProtocolVersion protocol;
  private boolean hasResultSet = false;

  public OperationHandle(OperationType opType, TProtocolVersion protocol) {
    super();
    this.opType = opType;
    this.protocol = protocol;
  }

  // dummy handle for ThriftCLIService
  public OperationHandle(TOperationHandle tOperationHandle) {
    this(tOperationHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
  }

  public OperationHandle(TOperationHandle tOperationHandle, TProtocolVersion protocol) {
    super(tOperationHandle.getOperationId());
    this.opType = OperationType.getOperationType(tOperationHandle.getOperationType());
    this.hasResultSet = tOperationHandle.isHasResultSet();
    this.protocol = protocol;
  }

  public OperationType getOperationType() {
    return opType;
  }

  public void setHasResultSet(boolean hasResultSet) {
    this.hasResultSet = hasResultSet;
  }

  public boolean hasResultSet() {
    return hasResultSet;
  }

  public TOperationHandle toTOperationHandle() {
    TOperationHandle tOperationHandle = new TOperationHandle();
    tOperationHandle.setOperationId(getHandleIdentifier().toTHandleIdentifier());
    tOperationHandle.setOperationType(opType.toTOperationType());
    tOperationHandle.setHasResultSet(hasResultSet);
    return tOperationHandle;
  }

  public TProtocolVersion getProtocolVersion() {
    return protocol;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((opType == null) ? 0 : opType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof OperationHandle)) {
      return false;
    }
    OperationHandle other = (OperationHandle) obj;
    if (opType != other.opType) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "OperationHandle [opType=" + opType + ", getHandleIdentifier()=" + getHandleIdentifier()
        + "]";
  }
}
