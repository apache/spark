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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class SessionStateSuite extends SparkFunSuite
    with BeforeAndAfterEach with BeforeAndAfterAll {

  /**
   * A shared SparkSession for all tests in this suite. Make sure you reset any changes to this
   * session as this is a singleton HiveSparkSession in HiveSessionStateSuite and it's shared
   * with all Hive test suites.
   */
  protected var activeSession: SparkSession = _

  override def beforeAll(): Unit = {
    activeSession = SparkSession.builder().master("local").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (activeSession != null) {
      activeSession.stop()
      activeSession = null
    }
    super.afterAll()
  }

  test("fork new session and inherit RuntimeConfig options") {
    val key = "spark-config-clone"
    try {
      activeSession.conf.set(key, "active")

      // inheritance
      val forkedSession = activeSession.cloneSession()
      assert(forkedSession ne activeSession)
      assert(forkedSession.conf ne activeSession.conf)
      assert(forkedSession.conf.get(key) == "active")

      // independence
      forkedSession.conf.set(key, "forked")
      assert(activeSession.conf.get(key) == "active")
      activeSession.conf.set(key, "dontcopyme")
      assert(forkedSession.conf.get(key) == "forked")
    } finally {
      activeSession.conf.unset(key)
    }
  }

  test("fork new session and inherit function registry and udf") {
    val testFuncName1 = "strlenScala"
    val testFuncName2 = "addone"
    try {
      activeSession.udf.register(testFuncName1, (_: String).length + (_: Int))
      val forkedSession = activeSession.cloneSession()

      // inheritance
      assert(forkedSession ne activeSession)
      assert(forkedSession.sessionState.functionRegistry ne
        activeSession.sessionState.functionRegistry)
      assert(forkedSession.sessionState.functionRegistry.lookupFunction(testFuncName1).nonEmpty)

      // independence
      forkedSession.sessionState.functionRegistry.dropFunction(testFuncName1)
      assert(activeSession.sessionState.functionRegistry.lookupFunction(testFuncName1).nonEmpty)
      activeSession.udf.register(testFuncName2, (_: Int) + 1)
      assert(forkedSession.sessionState.functionRegistry.lookupFunction(testFuncName2).isEmpty)
    } finally {
      activeSession.sessionState.functionRegistry.dropFunction(testFuncName1)
      activeSession.sessionState.functionRegistry.dropFunction(testFuncName2)
    }
  }

  test("fork new session and inherit experimental methods") {
    val originalExtraOptimizations = activeSession.experimental.extraOptimizations
    val originalExtraStrategies = activeSession.experimental.extraStrategies
    try {
      object DummyRule1 extends Rule[LogicalPlan] {
        def apply(p: LogicalPlan): LogicalPlan = p
      }
      object DummyRule2 extends Rule[LogicalPlan] {
        def apply(p: LogicalPlan): LogicalPlan = p
      }
      val optimizations = List(DummyRule1, DummyRule2)
      activeSession.experimental.extraOptimizations = optimizations
      val forkedSession = activeSession.cloneSession()

      // inheritance
      assert(forkedSession ne activeSession)
      assert(forkedSession.experimental ne activeSession.experimental)
      assert(forkedSession.experimental.extraOptimizations.toSet ==
        activeSession.experimental.extraOptimizations.toSet)

      // independence
      forkedSession.experimental.extraOptimizations = List(DummyRule2)
      assert(activeSession.experimental.extraOptimizations == optimizations)
      activeSession.experimental.extraOptimizations = List(DummyRule1)
      assert(forkedSession.experimental.extraOptimizations == List(DummyRule2))
    } finally {
      activeSession.experimental.extraOptimizations = originalExtraOptimizations
      activeSession.experimental.extraStrategies = originalExtraStrategies
    }
  }

  test("fork new sessions and run query on inherited table") {
    def checkTableExists(sparkSession: SparkSession): Unit = {
      QueryTest.checkAnswer(sparkSession.sql(
        """
          |SELECT x.str, COUNT(*)
          |FROM df x JOIN df y ON x.str = y.str
          |GROUP BY x.str
        """.stripMargin),
        Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
    }

    val spark = activeSession
    // Cannot use `import activeSession.implicits._` due to the compiler limitation.
    import spark.implicits._

    try {
      activeSession
        .createDataset[(Int, String)](Seq(1, 2, 3).map(i => (i, i.toString)))
        .toDF("int", "str")
        .createOrReplaceTempView("df")
      checkTableExists(activeSession)

      val forkedSession = activeSession.cloneSession()
      assert(forkedSession ne activeSession)
      assert(forkedSession.sessionState ne activeSession.sessionState)
      checkTableExists(forkedSession)
      checkTableExists(activeSession.cloneSession()) // ability to clone multiple times
      checkTableExists(forkedSession.cloneSession()) // clone of clone
    } finally {
      activeSession.sql("drop table df")
    }
  }

  test("fork new session and inherit reference to SharedState") {
    val forkedSession = activeSession.cloneSession()
    assert(activeSession.sharedState eq forkedSession.sharedState)
  }
}
