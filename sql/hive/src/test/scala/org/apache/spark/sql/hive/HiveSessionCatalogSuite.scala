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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.SimpleFunctionRegistry
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.internal.SQLConf

class HiveSessionCatalogSuite extends SessionCatalogSuite {

  test("clone HiveSessionCatalog") {
    val hiveSession = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
    assert(hiveSession.sessionState.catalog.isInstanceOf[HiveSessionCatalog])
    val original = hiveSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog]

    val tempTable1 = Range(1, 10, 1, 10)
    original.createTempView("copytest1", tempTable1, overrideIfExists = false)

    // check if tables copied over
    val clone = original.clone(
      hiveSession,
      new SQLConf,
      new Configuration(),
      new SimpleFunctionRegistry,
      CatalystSqlParser)
    assert(original ne clone)
    assert(clone.getTempView("copytest1") == Option(tempTable1))

    // check if clone and original independent
    clone.dropTable(TableIdentifier("copytest1"), ignoreIfNotExists = false, purge = false)
    assert(original.getTempView("copytest1") == Option(tempTable1))

    val tempTable2 = Range(1, 20, 2, 10)
    original.createTempView("copytest2", tempTable2, overrideIfExists = false)
    assert(clone.getTempView("copytest2").isEmpty)
  }
}
