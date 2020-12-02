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
package org.apache.spark.sql.hive.thriftserver

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.hive.service.cli.OperationHandle
import org.apache.hive.service.cli.operation.GetCatalogsOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.hive.service.rpc.thrift.{THandleIdentifier, TOperationHandle, TOperationType}

class GetCatalogsOperationMock(parentSession: HiveSession)
  extends GetCatalogsOperation(parentSession) {

  override def runInternal(): Unit = {}

  override def getHandle: OperationHandle = {
    val uuid: UUID = UUID.randomUUID()
    val tHandleIdentifier: THandleIdentifier = new THandleIdentifier()
    tHandleIdentifier.setGuid(getByteBufferFromUUID(uuid))
    tHandleIdentifier.setSecret(getByteBufferFromUUID(uuid))
    val tOperationHandle: TOperationHandle = new TOperationHandle()
    tOperationHandle.setOperationId(tHandleIdentifier)
    tOperationHandle.setOperationType(TOperationType.GET_TYPE_INFO)
    tOperationHandle.setHasResultSetIsSet(false)
    new OperationHandle(tOperationHandle)
  }

  private def getByteBufferFromUUID(uuid: UUID): Array[Byte] = {
    val bb: ByteBuffer = ByteBuffer.wrap(new Array[Byte](16))
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    bb.array
  }
}
