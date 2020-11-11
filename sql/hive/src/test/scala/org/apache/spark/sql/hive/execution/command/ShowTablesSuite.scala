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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.hive.test.TestHive

class ShowTablesSuite extends v1.ShowTablesSuite {
  override def version: String = "Hive V1"
  override def defaultUsing: String = "USING HIVE"
  override protected val spark: SparkSession = TestHive.sparkSession
  protected override def beforeAll(): Unit = {}

  test("show Hive tables") {
    withTable("show1a", "show2b") {
      sql("CREATE TABLE show1a(c1 int)")
      sql("CREATE TABLE show2b(c2 int)")
      runShowTablesSql(
        "SHOW TABLES IN default 'show1*'",
        ShowRow("default", "show1a", false) :: Nil)
      runShowTablesSql(
        "SHOW TABLES IN default 'show1*|show2*'",
        ShowRow("default", "show1a", false) ::
          ShowRow("default", "show2b", false) :: Nil)
      runShowTablesSql(
        "SHOW TABLES 'show1*|show2*'",
        ShowRow("default", "show1a", false) ::
          ShowRow("default", "show2b", false) :: Nil)
      assert(sql("SHOW TABLES").count() >= 2)
      assert(sql("SHOW TABLES IN default").count() >= 2)
    }
  }
}
