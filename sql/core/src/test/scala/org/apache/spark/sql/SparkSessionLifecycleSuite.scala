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

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.UI.UI_ENABLED


/**
 * Test cases for the lifecycle of a [[SparkSession]].
 */
class SparkSessionLifecycleSuite extends SparkFunSuite {
  test("test SparkContext stopped when last SparkSession is stopped ") {
    val session1 = SparkSession.builder()
      .master("local")
      .config(UI_ENABLED.key, value = false)
      .config("some-config", "a")
      .getOrCreate()

    assert(!session1.sparkContext.isStopped)

    val session2 = SparkSession.builder()
      .master("local")
      .config(UI_ENABLED.key, value = false)
      .config("some-config", "b")
      .getOrCreate()

    session1.stop()
    session2.stop()
    assert(session1.sparkContext.isStopped)
  }

  test("test SparkContext is not stopped when other sessions exist") {
    val session1 = SparkSession.builder()
      .master("local")
      .config(UI_ENABLED.key, value = false)
      .config("some-config", "a")
      .getOrCreate()

    assert(!session1.sparkContext.isStopped)

    val session2 = SparkSession.builder()
      .master("local")
      .config(UI_ENABLED.key, value = false)
      .config("some-config", "b")
      .getOrCreate()

    session1.stop()
    assert(!session1.sparkContext.isStopped)
  }
}
