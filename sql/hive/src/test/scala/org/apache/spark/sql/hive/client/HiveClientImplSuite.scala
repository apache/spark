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

package org.apache.spark.sql.hive.client

import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hive.{HiveUtils, StaticInitFlags, StaticInitInputFormat, StaticInitOutputFormat}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class HiveClientImplSuite extends SparkFunSuite {

  test("SPARK-59330: toHiveTable skips the format class static initializer when " +
    "spark.sql.hive.initializeMetastoreFormatClasses is false") {
    // Both call sites are exercised: toHiveTable resolves the input format via toInputFormat and
    // the output format via toOutputFormat, so each format class has its own flag.
    val table = CatalogTable(
      identifier = TableIdentifier("t", Some("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(
        inputFormat = Some(classOf[StaticInitInputFormat].getName),
        outputFormat = Some(classOf[StaticInitOutputFormat].getName)),
      schema = new StructType().add("a", "int"))

    def toHiveTableWith(initialize: Boolean): Unit = {
      val conf = new SQLConf()
      conf.setConf(HiveUtils.INITIALIZE_METASTORE_FORMAT_CLASSES, initialize)
      SQLConf.withExistingConf(conf) {
        HiveClientImpl.toHiveTable(table)
      }
    }

    // The false half must run first: class initialization is one-way per JVM. Resolving the
    // format class names without initializing them must not run their static initializers.
    toHiveTableWith(initialize = false)
    assert(!StaticInitFlags.inputFormatInitialized)
    assert(!StaticInitFlags.outputFormatInitialized)

    // With initialization enabled (the default), resolving the class names runs the initializers.
    toHiveTableWith(initialize = true)
    assert(StaticInitFlags.inputFormatInitialized)
    assert(StaticInitFlags.outputFormatInitialized)
  }

  test("SPARK-21529: a clear error is raised for an unsupported Hive union type") {
    val column = new FieldSchema("c", "uniontype<int,string>", null)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        HiveClientImpl.fromHiveColumn(column)
      },
      condition = "UNSUPPORTED_HIVE_TYPE",
      parameters = Map(
        "fieldType" -> "\"UNIONTYPE<INT,STRING>\"",
        "fieldName" -> "`c`"))
  }

  test("SPARK-21529: a Hive union type nested in a struct is detected") {
    val column = new FieldSchema("c", "struct<a:uniontype<int,string>>", null)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        HiveClientImpl.fromHiveColumn(column)
      },
      condition = "UNSUPPORTED_HIVE_TYPE",
      parameters = Map(
        "fieldType" -> "\"STRUCT<A:UNIONTYPE<INT,STRING>>\"",
        "fieldName" -> "`c`"))
  }
}
