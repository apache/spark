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

import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.HiveUtils

class HiveCliSessionStateSuite extends SparkFunSuite {

  test("CliSessionState will be reused") {
    val hiveConf = new HiveConf(classOf[SessionState])
    val sessionState: SessionState = new CliSessionState(hiveConf)
    SessionState.start(sessionState)
    doTest(sessionState, true)
  }

  test("SessionState will not be reused") {
    val hiveConf = new HiveConf(classOf[SessionState])
    HiveUtils.newTemporaryConfiguration(useInMemoryDerby = false).foreach {
      case (key, value) => hiveConf.set(key, value)
    }
    val sessionState: SessionState = new SessionState(hiveConf)
    doTest(sessionState, false)

  }

  def doTest(state: SessionState, expected: Boolean): Unit = {

    SessionState.start(state)
    val s1 = SessionState.get

    val sparkConf = new SparkConf()
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val hiveClient = HiveUtils.newClientForMetadata(sparkConf, hadoopConf)

    assert((hiveClient.toString == s1.toString) === expected)
  }
}
