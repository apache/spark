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

package org.apache.spark

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.Suite

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll with BeforeAndAfterEach { self: Suite =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  // SPARK-49647: use `SparkConf()` instead of `SparkConf(false)` because we want to
  // load defaults from system properties and the classpath, including default test
  // settings specified in the SBT and Maven build definitions.
  val conf: SparkConf = new SparkConf()

  /**
   * Initialize the [[SparkContext]].  Generally, this is just called from beforeAll; however, in
   * test using styles other than FunSuite, there is often code that relies on the session between
   * test group constructs and the actual tests, which may need this session.  It is purely a
   * semantic difference, but semantically, it makes more sense to call 'initializeContext' between
   * a 'describe' and an 'it' call than it does to call 'beforeAll'.
   */
  protected def initializeContext(): Unit = {
    if (null == _sc) {
      _sc = new SparkContext(
        "local[4]", "test", conf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName))
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeContext()
  }

  override def afterAll(): Unit = {
    try {
      LocalSparkContext.stop(_sc)
      _sc = null
    } finally {
      super.afterAll()
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    DebugFilesystem.assertNoOpenStreams()
  }
}
