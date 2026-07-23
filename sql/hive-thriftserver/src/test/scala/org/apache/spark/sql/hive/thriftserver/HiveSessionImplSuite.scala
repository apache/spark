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

import java.io.{ByteArrayOutputStream, PrintStream}
import java.lang.reflect.InvocationTargetException

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.OperationHandle
import org.apache.hive.service.cli.operation.{GetCatalogsOperation, Operation, OperationManager}
import org.apache.hive.service.cli.session.{HiveSession, HiveSessionImpl, SessionManager}
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.StaticSQLConf

class HiveSessionImplSuite extends SparkFunSuite {
  private var session: HiveSessionImpl = _
  private var operationManager: OperationManagerMock = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val sessionManager = new SessionManager(null)
    operationManager = new OperationManagerMock()

    session = new HiveSessionImpl(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
      "",
      "",
      new HiveConf(),
      ""
    )
    session.setSessionManager(sessionManager)
    session.setOperationManager(operationManager)

    session.open(Map.empty[String, String].asJava)
  }

  test("SPARK-31387 - session.close() closes all sessions regardless of thrown exceptions") {
    val operationHandle1 = session.getCatalogs
    val operationHandle2 = session.getCatalogs

    session.close()

    assert(operationManager.getCalledHandles.contains(operationHandle1))
    assert(operationManager.getCalledHandles.contains(operationHandle2))
  }

  private def withSystemPropSession[T](allowSettingSystemProperties: Boolean)(f: => T): T = {
    HiveSessionImpl.setAllowSettingSystemProperties(allowSettingSystemProperties)
    val conf = new HiveConf()
    val sessionImpl = new HiveSessionImpl(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1, "", "", conf, "")
    sessionImpl.setSessionManager(new SessionManager(null))
    sessionImpl.setOperationManager(new OperationManagerMock())
    sessionImpl.open(Map.empty[String, String].asJava)
    // open() does not call SessionState.start(), so err is not initialized in this unit test.
    SessionState.get().err = new PrintStream(new ByteArrayOutputStream())
    try f finally {
      sessionImpl.close()
      // Restore default-deny so other tests don't inherit the relaxed state.
      HiveSessionImpl.setAllowSettingSystemProperties(false)
    }
  }

  test("SPARK-57441: system:* variables can not be set by default") {
    val key = "spark.test.HiveSessionImplSuite.systemPrefixDefault"
    try {
      val rc = withSystemPropSession(allowSettingSystemProperties = false) {
        HiveSessionImpl.setVariable("system:" + key, "value")
      }
      assert(rc == 1)
      assert(System.getProperty(key) == null)
    } finally {
      System.clearProperty(key)
    }
  }

  test("SPARK-57441: system:* variables can be set when the legacy conf is enabled") {
    val key = "spark.test.HiveSessionImplSuite.systemPrefixLegacy"
    try {
      val rc = withSystemPropSession(allowSettingSystemProperties = true) {
        HiveSessionImpl.setVariable("system:" + key, "value")
      }
      assert(rc == 0)
      assert(System.getProperty(key) == "value")
    } finally {
      System.clearProperty(key)
    }
  }

  test("SPARK-57480: set:hiveconf cannot flip the legacy flag mid-session to bypass " +
      "the system:* gate") {
    val key = "spark.test.HiveSessionImplSuite.bypassRegression"
    val flagKey = StaticSQLConf.LEGACY_HIVE_THRIFT_SERVER_ALLOW_SETTING_SYSTEM_PROPERTIES.key
    try {
      withSystemPropSession(allowSettingSystemProperties = false) {
        // The system: gate must not read its toggle from the per-session HiveConf, since
        // `set:hiveconf:` directives write to that same HiveConf. Verify a client cannot
        // flip the flag mid-session via `set:hiveconf:<flag-key>=true` and then bypass
        // the gate. The hiveconf write itself is permitted (no-op for the gate).
        HiveSessionImpl.setVariable("hiveconf:" + flagKey, "true")
        val rcSystem = HiveSessionImpl.setVariable("system:" + key, "value")
        assert(rcSystem == 1, s"system:* should still be rejected, got rc=$rcSystem")
        assert(System.getProperty(key) == null,
          s"system:* should not have been set, got ${System.getProperty(key)}")
      }
    } finally {
      System.clearProperty(key)
    }
  }
}

class OperationManagerMock extends OperationManager {
  private val calledHandles: mutable.Set[OperationHandle] = new mutable.HashSet[OperationHandle]()

  override def newGetCatalogsOperation(parentSession: HiveSession): GetCatalogsOperation = {
    val operation = new GetCatalogsOperationMock(parentSession)
    try {
      val m = classOf[OperationManager].getDeclaredMethod("addOperation", classOf[Operation])
      m.setAccessible(true)
      m.invoke(this, operation)
    } catch {
      case e@(_: NoSuchMethodException | _: IllegalAccessException |
              _: InvocationTargetException) =>
        throw new RuntimeException(e)
    }
    operation
  }

  override def closeOperation(opHandle: OperationHandle): Unit = {
    calledHandles.add(opHandle)
    throw new RuntimeException
  }

  def getCalledHandles: mutable.Set[OperationHandle] = calledHandles
}

