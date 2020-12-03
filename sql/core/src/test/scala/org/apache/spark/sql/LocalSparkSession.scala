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

import _root_.io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

/** Manages a local `spark` {@link SparkSession} variable, correctly stopping it after each test. */
trait LocalSparkSession extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE)
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override def afterEach(): Unit = {
    try {
      LocalSparkSession.stop(spark)
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      spark = null
    } finally {
      super.afterEach()
    }
  }
}

object LocalSparkSession {
  def stop(spark: SparkSession): Unit = {
    if (spark != null) {
      spark.stop()
    }
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSparkSession[T](sc: SparkSession)(f: SparkSession => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

}
