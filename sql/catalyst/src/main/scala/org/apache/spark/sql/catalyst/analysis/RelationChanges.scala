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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.{RELATION_CHANGES, TreePattern}
import org.apache.spark.sql.connector.catalog.ChangelogInfo

/**
 * A logical node used to query Change Data Capture (CDC) changes for a table relation.
 *
 * This is an unresolved node created by the parser when it encounters a `CHANGES` clause,
 * or by the DataFrame API when `DataFrameReader.changes()` / `DataStreamReader.changes()` is
 * called. During analysis, it is resolved by loading a `Changelog` from the catalog and wrapping
 * it in a `ChangelogTable`.
 *
 * Note: `relation` is a constructor field, not a tree child (this node extends
 * [[UnresolvedLeafNode]]). Tree traversals like `transformUp` will not visit `relation`.
 *
 * @param relation the table relation (typically an [[UnresolvedRelation]])
 * @param changelogInfo the CDC query parameters (range, deduplication mode, etc.)
 */
case class RelationChanges(
    relation: LogicalPlan,
    changelogInfo: ChangelogInfo) extends UnresolvedLeafNode {
  override val nodePatterns: Seq[TreePattern] = Seq(RELATION_CHANGES)
}
