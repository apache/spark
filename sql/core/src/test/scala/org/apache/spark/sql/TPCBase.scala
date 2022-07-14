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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait TPCBase extends SharedSparkSession {

  protected def injectStats: Boolean = false

  override protected def sparkConf: SparkConf = {
    if (injectStats) {
      super.sparkConf
        .set(SQLConf.MAX_TO_STRING_FIELDS, Int.MaxValue)
        .set(SQLConf.CBO_ENABLED, true)
        .set(SQLConf.PLAN_STATS_ENABLED, true)
        .set(SQLConf.JOIN_REORDER_ENABLED, true)
    } else {
      super.sparkConf.set(SQLConf.MAX_TO_STRING_FIELDS, Int.MaxValue)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTables()
  }

  override def afterAll(): Unit = {
    dropTables()
    super.afterAll()
  }

  protected def createTables(): Unit

  protected def dropTables(): Unit
}
