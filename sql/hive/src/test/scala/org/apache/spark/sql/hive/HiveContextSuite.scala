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

package org.apache.spark.sql.hive


import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.hive.test.TestHive


class HiveContextSuite extends FunSuite with BeforeAndAfterAll {

  private val testHiveContext = TestHive
  private val testSparkContext = TestHive.sparkContext

  override def afterAll(): Unit = {
    HiveContext.setLastInstantiatedContext(testHiveContext)
  }

  test("getOrCreate instantiates HiveContext") {
    HiveContext.clearLastInstantiatedContext()
    val hiveContext = HiveContext.getOrCreate(testSparkContext)
    assert(hiveContext != null, "HiveContext.getOrCreate returned null")
    assert(HiveContext.getOrCreate(testSparkContext).eq(hiveContext),
      "HiveContext created by SQLContext.getOrCreate not returned by SQLContext.getOrCreate")
  }

  test("getOrCreate gets last explicitly instantiated HiveContext") {
    HiveContext.clearLastInstantiatedContext()
    val hiveContext = new HiveContext(testSparkContext)
    assert(HiveContext.getOrCreate(testSparkContext) != null,
      "HiveContext.getOrCreate after explicitly created SQLContext returned null")
    assert(HiveContext.getOrCreate(testSparkContext).eq(hiveContext),
      "HiveContext.getOrCreate after explicitly created SQLContext did not return the context")
  }
}
