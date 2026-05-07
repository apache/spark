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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.trees.TreeNodeTag

/**
 * Object used to store single-pass resolver related tags.
 */
object ResolverTag {

  /**
   * Tag used to mark [[Project]] nodes added for expression ID deduplication.
   */
  val PROJECT_FOR_EXPRESSION_ID_DEDUPLICATION =
    TreeNodeTag[Unit]("project_for_expression_id_deduplication")

  /**
   * Tag used to mark a node after resolving it to avoid traversing into its subtree twice.
   */
  val SINGLE_PASS_SUBTREE_BOUNDARY =
    TreeNodeTag[Unit]("single_pass_subtree_boundary")

  /**
   * Tag used to determine whether a node is an LCA.
   */
  val SINGLE_PASS_IS_LCA =
    TreeNodeTag[Unit]("single_pass_is_lca")

  /**
   * Tag used to mark the operator as the top-most operator in a query or a view.
   */
  val TOP_LEVEL_OPERATOR =
    TreeNodeTag[Unit]("top_level_operator")
}
