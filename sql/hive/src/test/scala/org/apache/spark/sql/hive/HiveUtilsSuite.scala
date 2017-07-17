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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.QueryTest

class HiveUtilsSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("newTemporaryConfiguration overwrites listener configurations") {
    Seq(true, false).foreach { useInMemoryDerby =>
      val conf = HiveUtils.newTemporaryConfiguration(useInMemoryDerby)
      assert(conf(ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname) === "")
      assert(conf(ConfVars.METASTORE_EVENT_LISTENERS.varname) === "")
      assert(conf(ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname) === "")
    }
  }

  test("CliSessionState will be reused") {
    val hiveConf = new HiveConf(classOf[SessionState])
    val sessionState: SessionState = new CliSessionState(hiveConf)
    SessionState.start(sessionState)
    val s1 = SessionState.get()
    assert(s1.isInstanceOf[CliSessionState])

    val sparkConf = new SparkConf()
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val metaHive = HiveUtils.newClientForMetadata(sparkConf, hadoopConf)
    assert(metaHive.getConf("spark.sql.hive.jars", "builtin") === "builtin")

    val s2 = SessionState.get()
    assert(s2.isInstanceOf[CliSessionState])
    assert(s1 === s2, "CliSessionState should be reused")
  }

  test("SessionState will not be reused") {
    val hiveConf = new HiveConf(classOf[SessionState])
    val sessionState: SessionState = new SessionState(hiveConf)
    SessionState.start(sessionState)

    val s1 = SessionState.get()
    assert(!s1.isInstanceOf[CliSessionState])

    val sparkConf = new SparkConf().set("spark.sql.hive.jars", "builtin")
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val metaHive = HiveUtils.newClientForMetadata(sparkConf, hadoopConf)

    assert(metaHive.getConf("spark.sql.hive.jars", "builtin") === "builtin")
    val s2 = SessionState.get()
    assert(s1 !== s2)

    val sparkConf2 = sparkConf.set("spark.sql.hive.jars", "maven")
    val hadoopConf2 = SparkHadoopUtil.get.newConfiguration(sparkConf2)
    val metaHive2 = HiveUtils.newClientForMetadata(sparkConf2, hadoopConf2)

    assert(metaHive2.getConf("spark.sql.hive.jars", "builtin") === "maven")
    val s3 = SessionState.get()
    assert(s1 !== s3)
    assert(s2 !== s3)
  }
}
