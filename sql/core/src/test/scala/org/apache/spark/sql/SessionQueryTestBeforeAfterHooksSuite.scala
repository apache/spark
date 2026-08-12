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

class SessionQueryTestBeforeAfterHooksSuite extends SessionQueryTest {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    checkAnswer(spark.sql("SELECT 1"), Seq(Row(1)))
  }

  override protected def beforeEach(): Unit = {
    checkAnswer(spark.sql("SELECT 1"), Seq(Row(1)))
    super.beforeEach()
    checkAnswer(spark.sql("SELECT 1"), Seq(Row(1)))
  }

  override protected def afterEach(): Unit = {
    checkAnswer(spark.sql("SELECT 1"), Seq(Row(1)))
    super.afterEach()
    checkAnswer(spark.sql("SELECT 1"), Seq(Row(1)))
  }

  override protected def afterAll(): Unit = {
    checkAnswer(spark.sql("SELECT 1"), Seq(Row(1)))
    super.afterAll()
  }

  test("assert spark is available in BeforeAndAfter hooks") {
    checkAnswer(spark.sql("SELECT 1"), Seq(Row(1)))
  }
}
