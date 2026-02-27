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

import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  NamedExpression,
  SortOrder,
  WindowFunctionType
}

/**
 * Type definitions for window specifications and related data structures.
 */
object WindowTypedefs {

  /**
   * Represents partition columns in a window specification.
   */
  type PartitionSpec = Seq[Expression]

  /**
   * Represents ordering columns in a window specification.
   */
  type OrderSpec = Seq[SortOrder]

  /**
   * Complete window specification including partition, ordering, and function type.
   */
  type WindowSpec = (PartitionSpec, OrderSpec, WindowFunctionType)

  /**
   * Tuple containing the specification ID and window expression aliases.
   */
  type WindowExpressionAliasesWithSpecId = (Int, Seq[NamedExpression])
}
