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

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.QueryTest

class HiveContextSuite extends QueryTest with TestHiveSingleton {
  test("newTemporaryConfiguration overwrites listener configurations") {
    val conf = HiveContext.newTemporaryConfiguration()
    assert(conf(ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname) === "")
    assert(conf(ConfVars.METASTORE_EVENT_LISTENERS.varname) === "")
    assert(conf(ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname) === "")
  }
}
