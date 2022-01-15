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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.{SessionState, SessionStateBuilder}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

class TestSparkSessionSuite extends SparkFunSuite {

  def getSparkSessionWithExtraConf(conf: SparkConf): SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName("TestSparkSessionSuite")
      .config(conf = conf)
      .getOrCreate()
  }

  test("default session is set in constructor") {
    val session = new TestSparkSession()
    assert(SparkSession.getDefaultSession.contains(session))
    session.stop()
  }

  test("[SPARK-37918] instance sessionBuilder with the specified construct") {
    val conf = new SparkConf()
    conf.set(CATALOG_IMPLEMENTATION, "org.apache.spark.sql.test.ExtensionSessionStateBuilder")
    val session = getSparkSessionWithExtraConf(conf)
    assert(session != null)
  }
}

private[sql] class ExtensionSessionStateBuilder(
    session: SparkSession,
    state: Option[SessionState])
  extends SessionStateBuilder(session, state) {

  def this() {
    this(null, None)
  }

  def this(session: SparkSession) {
    this(session, None)
  }

  def this(state: Option[SessionState]) {
    this(null, state)
  }

  override def newBuilder: NewBuilder = new ExtensionSessionStateBuilder(_, _)
}
