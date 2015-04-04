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

package org.apache.spark.util

import java.util.Properties

import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 * Mixin for automatically resetting system properties that are modified in ScalaTest tests.
 * This resets the properties after each individual test.
 *
 * The order in which fixtures are mixed in affects the order in which they are invoked by tests.
 * If we have a suite `MySuite extends FunSuite with Foo with Bar`, then
 * Bar's `super` is Foo, so Bar's beforeEach() will and afterEach() methods will be invoked first
 * by the rest runner.
 *
 * This means that ResetSystemProperties should appear as the last trait in test suites that it's
 * mixed into in order to ensure that the system properties snapshot occurs as early as possible.
 * ResetSystemProperties calls super.afterEach() before performing its own cleanup, ensuring that
 * the old properties are restored as late as possible.
 *
 * See the "Composing fixtures by stacking traits" section at
 * http://www.scalatest.org/user_guide/sharing_fixtures for more details about this pattern.
 */
private[spark] trait ResetSystemProperties extends BeforeAndAfterEach { this: Suite =>
  var oldProperties: Properties = null

  override def beforeEach(): Unit = {
    // we need SerializationUtils.clone instead of `new Properties(System.getProperties()` because
    // the later way of creating a copy does not copy the properties but it initializes a new
    // Properties object with the given properties as defaults. They are not recognized at all
    // by standard Scala wrapper over Java Properties then.
    oldProperties = SerializationUtils.clone(System.getProperties)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      System.setProperties(oldProperties)
      oldProperties = null
    }
  }
}
