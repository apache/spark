package org.apache.sparktest

import org.scalatest.Tag

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


/**
 * ScalaTest tags, that can be applied to tests to allow test inclusion & exclusion.  See
 * http://www.scalatest.org/user_guide/tagging_your_tests
 */
object TestTags {


  /**
   * Label a test case as an Integration Test.  Note that in the sbt build, all Integration Tests
   * can be skipped with "unit" profile, eg by running "~unit:test-quick" or "~unit:test-only ..."
   */
  object IntegrationTest extends Tag("org.apache.sparktest.tags.IntegrationTest")

  /**
   * This tag can be useful when you want to run only one test case in a suite.  You can label the
   * test as active, and then only run tests with this label using eg.
   * "~test-only *PairRDDFunctionSuite -- -n Active"
   * this can save a lot of time in your development loop to focus on only a few important tests
   * out of a big suite.
   *
   * Note that this tests should never be checked in with this tag -- its a utility to use only
   * during development
   */
  object ActiveTag extends Tag("Active")
}
