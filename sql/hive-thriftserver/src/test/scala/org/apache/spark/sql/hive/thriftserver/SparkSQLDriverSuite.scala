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

package org.apache.spark.sql.hive.thriftserver

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart

class SparkSQLDriverSuite extends SparkFunSuite {
  private var driver: SparkSQLDriver = _

  override protected def beforeEach(): Unit = {
    System.setProperty("spark.master", "local")
    SparkSQLEnv.init()
    driver = new SparkSQLDriver()
  }

  override protected def afterEach(): Unit = {
    driver.destroy()
    driver.close()
    SparkSQLEnv.stop()
  }

  test("Avoid wrapped in withNewExecutionId twice when run SQL with side effects") {
    val listener = new TestListener
    driver.context.sparkContext.addSparkListener(listener)
    try {
      val sqls = Seq(
        "CREATE TABLE t AS SELECT 1 AS i",
        "SELECT * FROM t",
        "DROP TABLE t")
      sqls.foreach(driver.run)

      assert(sqls.size === listener.sqlStartEvents.size)
      sqls.zip(listener.sqlStartEvents).foreach { case (sql, event) =>
        assert(sql === event.description)
      }
    } finally {
      driver.context.sparkContext.removeSparkListener(listener)
    }
  }

  class TestListener extends SparkListener {
    val sqlStartEvents = new ListBuffer[SparkListenerSQLExecutionStart]()

    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: SparkListenerSQLExecutionStart =>
        sqlStartEvents += e
      case _ =>
    }
  }
}
