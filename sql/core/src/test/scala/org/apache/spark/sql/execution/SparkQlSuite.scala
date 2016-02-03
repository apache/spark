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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.plans.PlanTest

class SparkQlSuite extends PlanTest {
  val parser = new SparkQl()

  test("create database") {
    parser.parsePlan("CREATE DATABASE IF NOT EXISTS database_name " +
      "COMMENT 'database_comment' LOCATION '/home/user/db' " +
      "WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")
  }

  test("create function") {
    parser.parsePlan("CREATE TEMPORARY FUNCTION helloworld as " +
      "'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar', " +
      "FILE 'path/to/file'")
  }
}
