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
package org.apache.spark.sql.test

import org.apache.spark.connect.proto
import org.apache.spark.sql.Column
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter

import java.nio.file.Path
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

/**
 * The basic testsuite the client tests should extend from.
 */
trait ConnectFunSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  // Borrowed from SparkFunSuite
  protected def getWorkspaceFilePath(first: String, more: String*): Path = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    java.nio.file.Paths.get(sparkHome, first +: more: _*)
  }

  protected def baseResourcePath: Path = {
    getWorkspaceFilePath(
      "sql",
      "connect",
      "client",
      "jvm",
      "src",
      "test",
      "resources").toAbsolutePath
  }

  protected def commonResourcePath: Path = {
    getWorkspaceFilePath("sql", "connect", "common", "src", "test", "resources").toAbsolutePath
  }

  protected def toExpr(c: Column): proto.Expression = ColumnNodeToProtoConverter(c.node)
}
