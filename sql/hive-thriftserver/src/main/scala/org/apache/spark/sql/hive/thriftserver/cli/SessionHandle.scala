/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli

import java.util.UUID

import org.apache.spark.service.cli.thrift.{TProtocolVersion, TSessionHandle}

class SessionHandle private(handleId: HandleIdentifier, protocol: TProtocolVersion)
  extends Handle(handleId) {

  def this(tSessionHandle: TSessionHandle) =
    this(new HandleIdentifier(tSessionHandle.getSessionId),
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)


  def this(tSessionHandle: TSessionHandle, protocol: TProtocolVersion) =
    this(new HandleIdentifier(tSessionHandle.getSessionId), protocol)

  def this(protocol: TProtocolVersion) = this(new HandleIdentifier(), protocol)

  def toTSessionHandle: TSessionHandle = {
    val tSessionHandle = new TSessionHandle
    tSessionHandle.setSessionId(getHandleIdentifier.toTHandleIdentifier)
    tSessionHandle
  }

  def getSessionId: UUID = getHandleIdentifier.getPublicId

  def getProtocolVersion: TProtocolVersion = protocol

  override def toString: String = "SessionHandle [" + getHandleIdentifier + "]"
}
