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

import java.util.UUID;

import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TSessionHandle;


/**
 * SessionHandle.
 *
 */
public class SessionHandle extends Handle {

  private final TProtocolVersion protocol;

  public SessionHandle(TProtocolVersion protocol) {
    this.protocol = protocol;
  }

  // dummy handle for ThriftCLIService
  public SessionHandle(TSessionHandle tSessionHandle) {
    this(tSessionHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
  }

  public SessionHandle(TSessionHandle tSessionHandle, TProtocolVersion protocol) {
    super(tSessionHandle.getSessionId());
    this.protocol = protocol;
  }

  public SessionHandle(HandleIdentifier handleId, TProtocolVersion protocol) {
    super(handleId);
    this.protocol = protocol;
  }

  public UUID getSessionId() {
    return getHandleIdentifier().getPublicId();
  }

  public TSessionHandle toTSessionHandle() {
    TSessionHandle tSessionHandle = new TSessionHandle();
    tSessionHandle.setSessionId(getHandleIdentifier().toTHandleIdentifier());
    return tSessionHandle;
  }

  public TProtocolVersion getProtocolVersion() {
    return protocol;
  }

  @Override
  public String toString() {
    return "SessionHandle [" + getHandleIdentifier() + "]";
  }
}
