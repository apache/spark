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

import java.util.TimeZone

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test._
import org.scalatest.BeforeAndAfterAll

/* Implicits */
import org.apache.spark.sql.TestData._
import org.apache.spark.sql.test.TestSQLContext._

class TmpSQLQuerySuite extends QueryTest with BeforeAndAfterAll {
  // Make sure the tables are loaded.
  TestData

  var origZone: TimeZone = _
  override protected def beforeAll() {
    origZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override protected def afterAll() {
    TimeZone.setDefault(origZone)
  }

  test("limit") {
    /*
    checkAnswer(
      sql("SELECT * FROM testData LIMIT 10"),
      testData.take(10).toSeq)
*/
    println("blah START")
    checkAnswer(
      sql("SELECT * FROM arrayData LIMIT 1"),
      arrayData.collect().take(1).toSeq)
    println("blah END")
/*
    checkAnswer(
      sql("SELECT * FROM mapData LIMIT 1"),
      mapData.collect().take(1).toSeq)
      */
  }

}
