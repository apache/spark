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

package org.apache.spark.sql.connector

import java.util.Collections

import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTable

/**
 * Runs the full [[DeleteFromTableSuiteBase]] test set against
 * [[org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationOnlyTable]] -- a row-level
 * operation table that does NOT mix in `SupportsDelete` / `TruncatableTable`. Every DELETE is
 * forced onto the row-level rewrite path, including predicates that fold to `TrueLiteral`
 * (which on the default fixture would be diverted to the metadata-only delete path).
 */
class DeleteFromTableNoDeleteFilterSuite extends DeleteFromTableSuiteBase {

  override protected def extraTableProps: java.util.Map[String, String] =
    Collections.singletonMap(InMemoryRowLevelOperationTable.SUPPORTS_DELETE_FILTER_PROP, "false")

  override protected def supportsDeleteByFilter: Boolean = false
}
