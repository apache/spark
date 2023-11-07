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

package org.apache.spark.sql.catalyst.util

trait ResolveDefaultColumnsUtils {
  // This column metadata indicates the default value associated with a particular table column that
  // is in effect at any given time. Its value begins at the time of the initial CREATE/REPLACE
  // TABLE statement with DEFAULT column definition(s), if any. It then changes whenever an ALTER
  // TABLE statement SETs the DEFAULT. The intent is for this "current default" to be used by
  // UPDATE, INSERT and MERGE, which evaluate each default expression for each row.
  val CURRENT_DEFAULT_COLUMN_METADATA_KEY = "CURRENT_DEFAULT"

  // This column metadata represents the default value for all existing rows in a table after a
  // column has been added. This value is determined at time of CREATE TABLE, REPLACE TABLE, or
  // ALTER TABLE ADD COLUMN, and never changes thereafter. The intent is for this "exist default" to
  // be used by any scan when the columns in the source row are missing data. For example, consider
  // the following sequence:
  // CREATE TABLE t (c1 INT)
  // INSERT INTO t VALUES (42)
  // ALTER TABLE t ADD COLUMNS (c2 INT DEFAULT 43)
  // SELECT c1, c2 FROM t
  // In this case, the final query is expected to return 42, 43. The ALTER TABLE ADD COLUMNS command
  // executed after there was already data in the table, so in order to enforce this invariant, we
  // need either (1) an expensive backfill of value 43 at column c2 into all previous rows, or (2)
  // indicate to each data source that selected columns missing data are to generate the
  // corresponding DEFAULT value instead. We choose option (2) for efficiency, and represent this
  // value as the text representation of a folded constant in the "EXISTS_DEFAULT" column metadata.
  val EXISTS_DEFAULT_COLUMN_METADATA_KEY = "EXISTS_DEFAULT"
}

object ResolveDefaultColumnsUtils extends ResolveDefaultColumnsUtils
