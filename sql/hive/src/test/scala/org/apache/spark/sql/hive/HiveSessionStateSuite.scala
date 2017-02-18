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

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.test.SharedSQLContext

class HiveSessionStateSuite  extends SparkFunSuite with SharedSQLContext {

  test("fork new session and inherit a copy of the session state") {
    val activeSession = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
    val forkedSession = activeSession.cloneSession()

    assert(forkedSession ne activeSession)
    assert(forkedSession.sessionState ne activeSession.sessionState)
    assert(forkedSession.sessionState.isInstanceOf[HiveSessionState])

    forkedSession.stop()
    activeSession.stop()
  }

  test("fork new session and inherit function registry and udf") {
    val activeSession = SparkSession.builder().master("local").getOrCreate()
    activeSession.udf.register("strlenScala", (_: String).length + (_: Int))
    val forkedSession = activeSession.cloneSession()

    assert(forkedSession ne activeSession)
    assert(forkedSession.sessionState.functionRegistry ne
      activeSession.sessionState.functionRegistry)
    assert(forkedSession.sessionState.functionRegistry.lookupFunction("strlenScala").nonEmpty)

    forkedSession.stop()
    activeSession.stop()
  }

}
