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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class SessionStateSuite extends SparkFunSuite with BeforeAndAfterEach {

  protected var activeSession: SparkSession = _

  protected def createSession(): Unit = {
    activeSession = SparkSession.builder().master("local").getOrCreate()
  }

  override def beforeEach(): Unit = {
    createSession()
  }

  override def afterEach(): Unit = {
    activeSession.stop()
  }

  test("fork new session and inherit a copy of the session state") {
    val forkedSession = activeSession.cloneSession()

    assert(forkedSession ne activeSession)
    assert(forkedSession.sessionState ne activeSession.sessionState)
    assert(forkedSession.conf ne activeSession.conf)
    // the rest of copying is tested individually for each field
  }

  test("fork new session and inherit RuntimeConfig options") {
    val key = "spark-config-clone"
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
  }

  test("fork new session and inherit function registry and udf") {
    activeSession.udf.register("strlenScala", (_: String).length + (_: Int))
    val forkedSession = activeSession.cloneSession()

    // inheritance
    assert(forkedSession ne activeSession)
    assert(forkedSession.sessionState.functionRegistry ne
      activeSession.sessionState.functionRegistry)
    assert(forkedSession.sessionState.functionRegistry.lookupFunction("strlenScala").nonEmpty)

    // independence
    forkedSession.sessionState.functionRegistry.dropFunction("strlenScala")
    assert(activeSession.sessionState.functionRegistry.lookupFunction("strlenScala").nonEmpty)
    activeSession.udf.register("addone", (_: Int) + 1)
    assert(forkedSession.sessionState.functionRegistry.lookupFunction("addone").isEmpty)
  }

  test("fork new session and inherit experimental methods") {
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
    assert(forkedSession.experimental.extraOptimizations.toSet ==
      activeSession.experimental.extraOptimizations.toSet)

    // independence
    forkedSession.experimental.extraOptimizations = List(DummyRule2)
    assert(activeSession.experimental.extraOptimizations == optimizations)
    activeSession.experimental.extraOptimizations = List(DummyRule1)
    assert(forkedSession.experimental.extraOptimizations == List(DummyRule2))
  }

  test("fork new session and run query on inherited table") {
    def checkTableExists(sparkSession: SparkSession): Unit = {
      QueryTest.checkAnswer(sparkSession.sql(
        """
          |SELECT x.str, COUNT(*)
          |FROM df x JOIN df y ON x.str = y.str
          |GROUP BY x.str
        """.stripMargin),
        Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
    }

    SparkSession.setActiveSession(activeSession)

    implicit val enc = Encoders.tuple(Encoders.scalaInt, Encoders.STRING)
    activeSession
      .createDataset[(Int, String)](Seq(1, 2, 3).map(i => (i, i.toString)))
      .toDF("int", "str")
      .createOrReplaceTempView("df")
    checkTableExists(activeSession)

    val forkedSession = activeSession.cloneSession()
    assert(forkedSession ne activeSession)
    SparkSession.setActiveSession(forkedSession)
    checkTableExists(forkedSession)

    SparkSession.clearActiveSession()
  }
}
