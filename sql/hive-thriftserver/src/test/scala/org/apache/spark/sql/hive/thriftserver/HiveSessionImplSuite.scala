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

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.OperationHandle
import org.apache.hive.service.cli.operation.{GetCatalogsOperation, OperationManager}
import org.apache.hive.service.cli.session.{HiveSessionImpl, SessionManager}
import org.mockito.Mockito.{mock, verify, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.SparkFunSuite

class HiveSessionImplSuite extends SparkFunSuite {
  private var session: HiveSessionImpl = _
  private var operationManager: OperationManager = _

  override def beforeAll() {
    super.beforeAll()

    session = new HiveSessionImpl(
      ThriftserverShimUtils.testedProtocolVersions.head,
      "",
      "",
      new HiveConf(),
      ""
    )
    val sessionManager = mock(classOf[SessionManager])
    session.setSessionManager(sessionManager)
    operationManager = mock(classOf[OperationManager])
    session.setOperationManager(operationManager)
    when(operationManager.newGetCatalogsOperation(session)).thenAnswer(
      (_: InvocationOnMock) => {
        val operation = mock(classOf[GetCatalogsOperation])
        when(operation.getHandle).thenReturn(mock(classOf[OperationHandle]))
        operation
      }
    )

    session.open(Map.empty[String, String].asJava)
  }

  test("SPARK-31387 - session.close() closes all sessions regardless of thrown exceptions") {
    val operationHandle1 = session.getCatalogs
    val operationHandle2 = session.getCatalogs

    when(operationManager.closeOperation(operationHandle1))
      .thenThrow(classOf[NullPointerException])
    when(operationManager.closeOperation(operationHandle2))
      .thenThrow(classOf[NullPointerException])

    session.close()

    verify(operationManager).closeOperation(operationHandle1)
    verify(operationManager).closeOperation(operationHandle2)
  }
}
