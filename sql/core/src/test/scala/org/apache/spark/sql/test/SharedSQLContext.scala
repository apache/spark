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

trait SharedSQLContext extends SQLTestUtils with SharedSparkSession {

  /**
   * Auto thread audit is turned off here intentionally and done manually.
   *
   * Without doing so thread audit will miss the [[SharedSparkSession]] related threads.
   */
  override protected val enableAutoThreadAudit = false

  protected override def beforeAll(): Unit = {
    doThreadPreAudit()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    doThreadPostAudit()
  }
}
