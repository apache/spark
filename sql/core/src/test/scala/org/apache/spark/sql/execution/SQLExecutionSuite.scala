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

package org.apache.spark.sql.execution

import org.apache.spark.sql.test.SharedSQLContext

class SQLExecutionSuite extends SharedSQLContext {
  import testImplicits._

  test("query execution IDs are not inherited across threads") {
    sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, "123")
    sparkContext.setLocalProperty("do-inherit-me", "some-value")
    var throwable: Option[Throwable] = None
    val thread = new Thread {
      override def run(): Unit = {
        try {
          assert(sparkContext.getLocalProperty("do-inherit-me") === "some-value")
          assert(sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY) === null)
        } catch {
          case t: Throwable =>
            throwable = Some(t)
        }
      }
    }
    thread.start()
    thread.join()
    throwable.foreach { t => throw t }
  }

  // This is the end-to-end version of the previous test.
  test("parallel query execution (SPARK-10548)") {
    (1 to 5).foreach { i =>
      // Scala's parallel collections spawns new threads as children of the existing threads.
      // We need to run this multiple times to ensure new threads are spawned. Without the fix
      // for SPARK-10548, this usually fails on the second try.
      val df = sparkContext.parallelize(1 to 5).map { i => (i, i) }.toDF("a", "b")
      (1 to 10).par.foreach { _ => df.count() }
    }
  }
}
