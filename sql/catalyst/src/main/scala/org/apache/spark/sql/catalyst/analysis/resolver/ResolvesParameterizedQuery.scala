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

import org.apache.spark.sql.catalyst.analysis.ParameterizedQuery
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SupervisingCommand}

/**
 * Shared resolution logic for [[ParameterizedQuery]] operators.
 */
trait ResolvesParameterizedQuery {

  /**
   * Pushes a [[ParameterizedQuery]] below a [[SupervisingCommand]]. The [[ParameterizedQuery]]
   * is preserved inside the command so it gets resolved during the deferred analysis pass in
   * [[SupervisingCommand.run]].
   *
   * The [[ParameterizedQuery]] is pushed down through any nested [[SupervisingCommand]]s.
   * Examples:
   *
   * 1. Parameters below EXPLAIN command:
   * {{{
   *  EXPLAIN SELECT :first;
   *
   *  -- Analyzed plan
   *  -- ExplainCommand 'NameParameterizedQuery [first], [1], SimpleMode
   * }}}
   *
   * 2. Parameters below DESCRIBE command:
   * {{{
   *  DESCRIBE QUERY SELECT :first;
   *
   *  -- Analyzed plan
   *  -- DescribeQueryCommand 'NameParameterizedQuery [first], [1]
   * }}}
   *
   * 3. Parameters below nested commands:
   * {{{
   *  EXPLAIN EXPLAIN SELECT :first;
   *
   *  -- Analyzed plan
   *  -- ExplainCommand ExplainCommand 'NameParameterizedQuery [a], [1], SimpleMode, SimpleMode
   * }}}
   */
  protected def pushParameterizedQueryBelowCommand(
      parameterizedQuery: ParameterizedQuery,
      command: SupervisingCommand): LogicalPlan = {
    command.withTransformedSupervisedPlan {
      case nestedCommand: SupervisingCommand =>
        pushParameterizedQueryBelowCommand(parameterizedQuery, nestedCommand)
      case supervisedPlan =>
        parameterizedQuery.withNewChildren(Seq(supervisedPlan))
    }
  }
}
