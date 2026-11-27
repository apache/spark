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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.internal.StaticSQLConf

/**
 * Tests for the SPARK-59118 startup guard: the Thrift Server refuses to start when
 * `hive.server2.enable.doAs` is on but impersonation does not reach executor-side data
 * access. The `init()` wiring is covered by SparkExecuteStatementOperationSuite so this
 * suite stays a plain SparkFunSuite.
 */
class HiveThriftServer2DoAsSuite extends SparkFunSuite {

  private def hiveConf(authType: String, doAs: Boolean): HiveConf = {
    val conf = new HiveConf()
    conf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, authType)
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, doAs)
    conf
  }

  private def check(conf: HiveConf, allowIneffectiveDoAs: Boolean = false): Unit = {
    HiveThriftServer2.failIfIneffectiveDoAs(conf, allowIneffectiveDoAs)
  }

  private def checkRefused(conf: HiveConf): SparkException = {
    val e = intercept[SparkException](check(conf))
    checkError(
      e,
      condition = "HIVE_THRIFT_SERVER_INEFFECTIVE_DOAS",
      sqlState = Some("42616"),
      parameters = Map(
        "doAsConf" -> ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname,
        "allowIneffectiveDoAsConf" ->
          StaticSQLConf.HIVE_THRIFT_SERVER_ALLOW_INEFFECTIVE_DOAS.key))
    e
  }

  // The auth types that establish a user identity worth impersonating.
  private val verifyingAuthTypes =
    AuthTypes.values().filterNot(Set(AuthTypes.NONE, AuthTypes.NOSASL)).map(_.getAuthName)

  test("SPARK-5159 / SPARK-59118 refuse to start when doAs is enabled but not enforced") {
    assert(checkRefused(hiveConf("KERBEROS", doAs = true)).getMessage.contains("SPARK-5159"))
    // Auth type matching is case-insensitive.
    checkRefused(hiveConf("kerberos", doAs = true))
  }

  test("SPARK-59118 refuse to start for every auth type that verifies the user") {
    // Guards against a new AuthTypes value silently landing on the permissive side.
    assert(verifyingAuthTypes.nonEmpty)
    verifyingAuthTypes.foreach { authType =>
      checkRefused(hiveConf(authType, doAs = true))
    }
  }

  test("SPARK-59118 does not refuse to start when auth type establishes no user identity") {
    Seq("NONE", "NOSASL", "none", "noSasl").foreach { authType =>
      check(hiveConf(authType, doAs = true))
    }
  }

  test("SPARK-59118 does not refuse to start when doAs is disabled") {
    verifyingAuthTypes.foreach { authType =>
      check(hiveConf(authType, doAs = false))
    }
  }

  test("SPARK-59118 does not refuse to start on an untouched Hive config") {
    // Hive defaults doAs to true and auth to NONE, so out-of-the-box servers must still start.
    check(new HiveConf())
  }

  test("SPARK-59118 allowIneffectiveDoAs=true permits an otherwise-refused config") {
    check(hiveConf("KERBEROS", doAs = true), allowIneffectiveDoAs = true)
  }

  test("SPARK-59118 an unrecognized or empty auth type is left to Hive's own error") {
    // HiveAuthFactory rejects these with "Unsupported authentication type", where the guard's
    // advice would not help. getVar returns "" (never null) for an explicitly empty value,
    // so this must not NPE.
    Seq("NOT_AN_AUTH_TYPE", "").foreach { authType =>
      check(hiveConf(authType, doAs = true))
    }
  }
}
