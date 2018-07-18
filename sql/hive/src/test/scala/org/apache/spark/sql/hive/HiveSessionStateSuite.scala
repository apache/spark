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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHiveSingleton

/**
 * Run all tests from `SessionStateSuite` with a Hive based `SessionState`.
 */
class HiveSessionStateSuite extends SessionStateSuite with TestHiveSingleton {

  override def beforeAll(): Unit = {
    // Reuse the singleton session
    super.beforeAll()
    activeSession = spark
  }

  override def afterAll(): Unit = {
    // Set activeSession to null to avoid stopping the singleton session
    activeSession = null
    super.afterAll()
  }

  test("Clone then newSession") {
    val sparkSession = hiveContext.sparkSession
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val oldValue = conf.get(ConfVars.METASTORECONNECTURLKEY.varname)
    sparkSession.cloneSession()
    sparkSession.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog]
      .client.newSession()
    val newValue = conf.get(ConfVars.METASTORECONNECTURLKEY.varname)
    assert(oldValue == newValue,
      "cloneSession and then newSession should not affect the Derby directory")
  }
}
