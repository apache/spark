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

package org.apache.spark.sql.catalyst

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf.NESTED_SCHEMA_PRUNING_ENABLED

/**
 * A PlanTest that ensures that all tests in this suite are run with nested schema pruning enabled.
 * Remove this trait once the default value of SQLConf.NESTED_SCHEMA_PRUNING_ENABLED is set to true.
 */
private[sql] trait SchemaPruningTest extends PlanTest with BeforeAndAfterAll {
  private var originalConfSchemaPruningEnabled = false

  override protected def beforeAll(): Unit = {
    originalConfSchemaPruningEnabled = conf.nestedSchemaPruningEnabled
    conf.setConf(NESTED_SCHEMA_PRUNING_ENABLED, true)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      conf.setConf(NESTED_SCHEMA_PRUNING_ENABLED, originalConfSchemaPruningEnabled)
    }
  }
}
