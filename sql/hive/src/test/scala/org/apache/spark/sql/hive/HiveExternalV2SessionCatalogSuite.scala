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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalogTableBaseSuite
import org.apache.spark.sql.hive.test.TestHiveSingleton


  class HiveExternalV2SessionCatalogTableSuite extends V2SessionCatalogTableBaseSuite
    with TestHiveSingleton {

  def excluded: Seq[String] = Seq(
    // Not supported in Hive catalog
    "alterTable: add nested column",
    "createTable: location",
    "alterTable: location",

    // Not supported in V2SessionCatalog
    "alterTable: rename top-level column",
    "alterTable: rename nested column",
    "alterTable: rename struct column",
    "alterTable: multiple changes",
    "alterTable: delete top-level column",
    "alterTable: delete nested column"
  )

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    if (excluded.contains(testName)) ()
    else super.test(testName, testTags: _*)(testFun)
  }
}
