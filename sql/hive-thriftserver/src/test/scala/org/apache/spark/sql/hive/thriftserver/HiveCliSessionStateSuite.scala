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

  def withSessionClear(f: () => Unit): Unit = {
    try f() finally SessionState.detachSession()
  }

  test("CliSessionState will be reused") {
    withSessionClear { () =>
      val hiveConf = new HiveConf(classOf[SessionState])
      HiveUtils.newTemporaryConfiguration(useInMemoryDerby = false).foreach {
        case (key, value) => hiveConf.set(key, value)
      }
      val sessionState: SessionState = new CliSessionState(hiveConf)
      SessionState.start(sessionState)
      val s1 = SessionState.get
      val sparkConf = new SparkConf()
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
      val s2 = HiveUtils.newClientForMetadata(sparkConf, hadoopConf).getState
      assert(s1 === s2)
      assert(s2.isInstanceOf[CliSessionState])
    }
  }

  test("SessionState will not be reused") {
    withSessionClear { () =>
      val sparkConf = new SparkConf()
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
      HiveUtils.newTemporaryConfiguration(useInMemoryDerby = false).foreach {
        case (key, value) => hadoopConf.set(key, value)
      }
      val hiveClient = HiveUtils.newClientForMetadata(sparkConf, hadoopConf)
      val s1 = hiveClient.getState
      val s2 = hiveClient.newSession().getState
      assert(s1 !== s2)
    }
  }
}
