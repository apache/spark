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

/**
 * Provides connect-compatible test utils to write suites that have 'connect variants':
 * {{{
 *   // in sql/core
 *   FooSuite extends SessionQueryTest { test("") { ... } }
 *
 *   // in sql/connect
 *   FooConnectSuite extends connect.SessionQueryTest
 * }}}
 *
 * While this trait internally uses a [[classic.SparkSession]] when executing tests,
 * it is exposed as a [[SparkSession sql.SparkSession]] to allow for overriding on the connect side.
 *
 * For classic-specific tests, use [[classic.SessionQueryTest]].
 *
 * For example usage, see the `ExampleSessionAgnosticSuite` example suites in sql/connect.
 */
trait SessionQueryTest
  extends SparkFunSuite
  with SessionQueryTestBase
  with SparkSessionBinder {

  override def isConnect: Boolean = false
}
