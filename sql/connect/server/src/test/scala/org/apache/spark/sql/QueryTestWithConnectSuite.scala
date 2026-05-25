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

import org.apache.spark.sql.test.connect.{QueryTest => ConnectQueryTest, SharedSparkSession => ConnectSharedSparkSession}

/**
 * Demonstrator: runs [[QueryTestSuite]] tests through a Connect session.
 *
 * This suite validates that the lifted [[QueryTestBase]] infrastructure works end-to-end with a
 * Connect-providing session trait.
 */
class QueryTestWithConnectSuite
  extends ConnectQueryTest
    with ConnectSharedSparkSession {

  test("SPARK-16940: checkAnswer should raise TestFailedException for wrong results") {
    intercept[org.scalatest.exceptions.TestFailedException] {
      checkAnswer(sql("SELECT 1"), Row(2) :: Nil)
    }
  }

  test("SPARK-51349: null string and true null are distinguished") {
    checkAnswer(sql("select case when id == 0 then struct('null') else struct(null) end s " +
      "from range(2)"),
      Seq(Row(Row(null)), Row(Row("null"))))
  }
}
