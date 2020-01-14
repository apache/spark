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

import org.apache.spark.sql.connector.ExternalCommandRunnableProvider
import org.apache.spark.sql.test.SharedSparkSession

class ExternalCommandRunnableProviderSuite extends SharedSparkSession {

  test("execute command") {
    System.setProperty("command", "hello")
    val parameters = Map("one" -> "1", "two" -> "2").asJava
    assert(System.getProperty("command") === "hello")
    val df = spark.read.format("cmdSource")
      .options(parameters)
      .executeCommand("hello")
    // executeCommand should execute the command eagerly
    assert(System.getProperty("command") === "world")
    val output = df.collect().head.getString(0)
    assert(output === Array("hello".reverse, parameters.toString()).mkString("\n"))
    System.clearProperty("command")
  }
}

class CommandRunnableDataSource extends DataSourceRegister with ExternalCommandRunnableProvider {
  override def shortName(): String = "cmdSource"

  override def executeCommand(command: String, parameters: java.util.Map[String, String])
    : Array[String] = {
    System.setProperty("command", "world")
    Array(command.reverse, parameters.toString())
  }
}
