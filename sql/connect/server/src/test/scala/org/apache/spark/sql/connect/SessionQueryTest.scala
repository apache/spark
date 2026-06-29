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
package org.apache.spark.sql.connect

import scala.util.matching.Regex

import org.apache.spark.sql

/**
 * Overrides test utils to implement 'connect variants' of suites declared in sql/core:
 * {{{
 *   // in sql/core
 *   FooSuite extends SessionQueryTest { test("") { ... } }
 *
 *   // in sql/connect
 *   FooConnectSuite extends FooSuite with connect.SessionQueryTest
 * }}}
 *
 * This trait overrides [[spark]] to use a [[SparkSession connect.SparkSession]], which executes
 * via the gRPC API using an in-process connect server.
 */
trait SessionQueryTest extends sql.SessionQueryTest with SparkSessionBinder {

  private val sortOperator: Regex = """\b(?:Photon)?Sort\b""".r

  /**
   * Approximates [[sql.SessionQueryTest.isDfSorted]] by inspecting the explain string.
   */
  override def isDfSorted(df: sql.DataFrame): Boolean = df match {
    case df: DataFrame => sortOperator.findFirstIn(df.explainString(extended = false)).isDefined
    case df => super.isDfSorted(df)
  }

  override def sessionType: String = "connect"
}
