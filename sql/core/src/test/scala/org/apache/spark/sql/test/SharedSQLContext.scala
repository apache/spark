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
   * Suites extending [[SharedSQLContext]] are sharing resources (eg. SparkSession) in their tests.
   * Such resources are initialized by the suite before thread audit takes thread snapshot and
   * cleaned up after the audit checks for possible leaks.
   *
   * 1. Init resources
   * 2. ThreadAudit pre step
   * 3. Test code
   * 4. ThreadAudit post step
   * 5. Destroy resources
   *
   * By turning auto thread audit off and doing it manually audit steps can be executed at the
   * proper place.
   *
   * 1. ThreadAudit pre step
   * 2. Init resources
   * 3. Test code
   * 4. Destroy resources
   * 5. ThreadAudit post step
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
