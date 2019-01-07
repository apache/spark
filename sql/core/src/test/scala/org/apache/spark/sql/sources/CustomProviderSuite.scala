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

package org.apache.spark.sql.sources

import org.apache.spark.SparkFunSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.test.SharedSQLContext

class CustomProviderSuite extends SparkFunSuite with SharedSQLContext {

  override protected def sparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.source.provider.fake1", "org.apache.spark.sql.sources.FakeSourceOne")
  }

  private def getProvidingClass(name: String): Class[_] =
    DataSource(
      sparkSession = spark,
      className = name
    ).providingClass

  test("fake source with custom provider") {
    assert(
      getProvidingClass("fake1") ===
      classOf[org.apache.spark.sql.sources.FakeSourceOne])
  }
}
