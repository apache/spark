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
import org.apache.spark.sql.test.SharedSQLContext

class SQLContextSuite extends SparkFunSuite with SharedSQLContext {

  override def afterAll(): Unit = {
    try {
      SQLContext.setLastInstantiatedContext(sqlContext)
    } finally {
      super.afterAll()
    }
  }

  test("getOrCreate instantiates SQLContext") {
    SQLContext.clearLastInstantiatedContext()
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    assert(sqlContext != null, "SQLContext.getOrCreate returned null")
    assert(SQLContext.getOrCreate(sparkContext).eq(sqlContext),
      "SQLContext created by SQLContext.getOrCreate not returned by SQLContext.getOrCreate")
  }

  test("getOrCreate gets last explicitly instantiated SQLContext") {
    SQLContext.clearLastInstantiatedContext()
    val sqlContext = new SQLContext(sparkContext)
    assert(SQLContext.getOrCreate(sparkContext) != null,
      "SQLContext.getOrCreate after explicitly created SQLContext returned null")
    assert(SQLContext.getOrCreate(sparkContext).eq(sqlContext),
      "SQLContext.getOrCreate after explicitly created SQLContext did not return the context")
  }
}
