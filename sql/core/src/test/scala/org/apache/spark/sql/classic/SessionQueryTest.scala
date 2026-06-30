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

package org.apache.spark.sql.classic

import org.apache.spark.sql

/**
 * Override of [[sql.SessionQueryTest]] that provides [[SparkSession classic.SparkSession]].
 *
 * Can be used to declare classic-specific tests:
 * {{{
 *   class FooSuite extends sql.SessionQueryTest {
 *     // shared classic/connect-agnostic testcases
 *   }
 *
 *   // no need to extend FooSuite as sql.SessionQueryTest
 *   // already executes shared tests via classic internally.
 *   class FooClassicSuite extends classic.SessionQueryTest {
 *     test("classic-only test") {
 *       // classic-only APIs are visible here
 *       spark.sessionState.conf
 *     }
 *   }
 * }}}
 */
trait SessionQueryTest extends sql.SessionQueryTest with SparkSessionBinder

