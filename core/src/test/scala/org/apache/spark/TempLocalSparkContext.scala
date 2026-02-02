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

import _root_.io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile

/**
 * Manages a local `sc` `SparkContext` variable, correctly stopping it after each test.
 *
 * Note: this class is a copy of [[LocalSparkContext]]. Why copy it? Reduce conflict. Because
 * many test suites use [[LocalSparkContext]] and overwrite some variable or function (e.g.
 * sc of LocalSparkContext), there occurs conflict when we refactor the `sc` as a new function.
 * After migrating all test suites that use [[LocalSparkContext]] to use
 * [[TempLocalSparkContext]], we will delete the original [[LocalSparkContext]] and rename
 * [[TempLocalSparkContext]] to [[LocalSparkContext]].
 */
trait TempLocalSparkContext extends BeforeAndAfterEach
  with BeforeAndAfterAll with Logging { self: Suite =>

  private var _conf: SparkConf = defaultSparkConf

  @transient private var _sc: SparkContext = _

  def conf: SparkConf = _conf

  /**
   * Currently, we are focusing on the reconstruction of LocalSparkContext, so this method
   * was created temporarily. When the migration work is completed, this method will be
   * renamed to `sc` and the variable `sc` will be deleted.
   */
  def sc: SparkContext = {
    if (_sc == null) {
      _sc = new SparkContext(conf)
    }
    _sc
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)
  }

  override def afterEach(): Unit = {
    try {
      resetSparkContext()
    } finally {
      super.afterEach()
    }
  }

  def resetSparkContext(): Unit = {
    TempLocalSparkContext.stop(_sc)
    ResourceProfile.clearDefaultProfile()
    _sc = null
    _conf = defaultSparkConf
  }

  private def defaultSparkConf: SparkConf = new SparkConf()
    .setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
}

object TempLocalSparkContext {
  def stop(sc: SparkContext): Unit = {
    if (sc != null) {
      sc.stop()
    }
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }
}
