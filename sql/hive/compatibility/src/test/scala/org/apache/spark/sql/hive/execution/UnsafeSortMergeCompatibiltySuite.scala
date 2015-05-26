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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.hive.test.TestHive

/**
 * Runs the test cases that are included in the hive distribution with sort merge join and
 * unsafe external sort enabled.
 */
class UnsafeSortMergeCompatibiltySuite extends SortMergeCompatibilitySuite {
  override def beforeAll() {
    super.beforeAll()
    TestHive.setConf(SQLConf.CODEGEN_ENABLED, "true")
    TestHive.setConf(SQLConf.UNSAFE_ENABLED, "true")
    TestHive.setConf(SQLConf.EXTERNAL_SORT, "true")
  }

  override def afterAll() {
    TestHive.setConf(SQLConf.CODEGEN_ENABLED, "false")
    TestHive.setConf(SQLConf.UNSAFE_ENABLED, "false")
    TestHive.setConf(SQLConf.EXTERNAL_SORT, "false")
    super.afterAll()
  }
}