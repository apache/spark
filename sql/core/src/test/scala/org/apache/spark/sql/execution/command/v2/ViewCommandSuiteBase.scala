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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.SparkConf
import org.apache.spark.sql.connector.catalog.InMemoryTableViewCatalog

/**
 * Settings for v2 view command test suites. Extends v2 [[CommandSuiteBase]] (so view tests
 * inherit `checkLocation` and the standard v2 `test_catalog` configuration), and additionally
 * wires `test_view_catalog` to [[InMemoryTableViewCatalog]] -- the catalog that the unified
 * `*SuiteBase` view tests under `command/` target via the `$catalog` placeholder.
 */
trait ViewCommandSuiteBase extends CommandSuiteBase {
  override def catalog: String = "test_view_catalog"

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalog", classOf[InMemoryTableViewCatalog].getName)

  /** Helper: returns the configured `InMemoryTableViewCatalog`. */
  protected def viewCatalog: InMemoryTableViewCatalog =
    spark.sessionState.catalogManager.catalog(catalog).asInstanceOf[InMemoryTableViewCatalog]
}
