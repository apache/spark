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
package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * This tests SQLConf.ADAPTIVE_FORCE_IF_SHUFFLE = true
 * By default SQLConf.ADAPTIVE_FORCE_IF_SHUFFLE = false.
 * New tests should be added in AdaptiveQueryExecSuite so that they get
 * tested with the option both on and off.
 *
 *
 * 3. test where new shuffle doesn't get a CSRE - coalesceShufflePartitionsEnabled?
 */
class AdaptivePreferShuffleSuite extends AdaptiveQueryExecSuite {
  var initVal = false
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initVal = SQLConf.get.getConf(SQLConf.ADAPTIVE_FORCE_IF_SHUFFLE)
    SQLConf.get.setConf(SQLConf.ADAPTIVE_FORCE_IF_SHUFFLE, true)
  }
  override protected def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.setConf(SQLConf.ADAPTIVE_FORCE_IF_SHUFFLE, initVal)
  }
}
