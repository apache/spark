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
import org.apache.logging.log4j.Level

import org.apache.spark.SparkFunSuite

/**
 * Tests for the SPARK-59118 startup warning: `hive.server2.enable.doAs` is accepted but
 * impersonation does not reach query execution. Spark 5.0 refuses to start instead.
 */
class HiveThriftServer2DoAsSuite extends SparkFunSuite {

  private def hiveConf(authType: String, doAs: Boolean): HiveConf = {
    val conf = new HiveConf()
    conf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, authType)
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, doAs)
    conf
  }

  /** Warnings naming the doAs conf, emitted while checking `conf`. */
  private def doAsWarnings(conf: HiveConf): Seq[String] = {
    val appender = new LogAppender("doAs impersonation warning")
    val canary = "doAs-appender-canary"
    withLogAppender(appender, level = Some(Level.WARN)) {
      HiveThriftServer2.warnIfIneffectiveDoAs(conf)
      logWarning(canary)
    }
    val messages = appender.loggingEvents
      .filter(_.getLevel == Level.WARN)
      .map(_.getMessage.getFormattedMessage)
    // Without this, a broken appender would make every "does not warn" case pass vacuously.
    assert(messages.exists(_.contains(canary)), "log appender captured nothing")
    messages.filter(_.contains(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname)).toSeq
  }

  // The auth types that establish a user identity worth impersonating.
  private val verifyingAuthTypes =
    AuthTypes.values().filterNot(Set(AuthTypes.NONE, AuthTypes.NOSASL)).map(_.getAuthName)

  test("SPARK-5159 / SPARK-59118 warn when doAs is enabled but not enforced") {
    val warnings = doAsWarnings(hiveConf("KERBEROS", doAs = true))
    assert(warnings.length == 1)
    assert(warnings.head.contains("SPARK-5159"))
  }

  test("SPARK-59118 warn for every auth type that verifies the user") {
    // Guards against a new AuthTypes value silently landing on the quiet side.
    assert(verifyingAuthTypes.nonEmpty)
    verifyingAuthTypes.foreach { authType =>
      assert(doAsWarnings(hiveConf(authType, doAs = true)).length == 1, s"no warning: $authType")
    }
  }

  test("SPARK-59118 does not warn when auth type establishes no user identity") {
    Seq("NONE", "NOSASL", "none", "noSasl").foreach { authType =>
      assert(doAsWarnings(hiveConf(authType, doAs = true)).isEmpty, s"warned: $authType")
    }
  }

  test("SPARK-59118 does not warn when doAs is disabled") {
    verifyingAuthTypes.foreach { authType =>
      assert(doAsWarnings(hiveConf(authType, doAs = false)).isEmpty, s"warned: $authType")
    }
  }

  test("SPARK-59118 does not warn on an untouched Hive config") {
    // Hive defaults doAs to true and auth to NONE, so a stock config must stay quiet.
    assert(doAsWarnings(new HiveConf()).isEmpty)
  }

  test("SPARK-59118 an unrecognized auth type warns") {
    assert(doAsWarnings(hiveConf("NOT_AN_AUTH_TYPE", doAs = true)).length == 1)
  }
}
