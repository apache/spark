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

package org.apache.spark.sql.sources

import scala.collection.JavaConverters._

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ExternalCommandRunnerSuite extends QueryTest with SharedSparkSession {
  test("execute command") {
    try {
      System.setProperty("command", "hello")
      assert(System.getProperty("command") === "hello")

      val options = Map("one" -> "1", "two" -> "2")
      val df = spark.executeCommand(classOf[FakeCommandRunner].getName, "world", options)
      // executeCommand should execute the command eagerly
      assert(System.getProperty("command") === "world")
      checkAnswer(df, Seq(Row("one"), Row("two")))
    } finally {
      System.clearProperty("command")
    }
  }
}

class FakeCommandRunner extends ExternalCommandRunner {

  override def executeCommand(command: String, options: CaseInsensitiveStringMap): Array[String] = {
    System.setProperty("command", command)
    options.keySet().iterator().asScala.toSeq.sorted.toArray
  }
}
