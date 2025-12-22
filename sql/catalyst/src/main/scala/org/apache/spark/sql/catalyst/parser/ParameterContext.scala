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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Context for passing parameter values to the SQL parser.
 * This allows parameter substitution to happen during the parsing phase
 * rather than during analysis.
 */
sealed trait ParameterContext

/**
 * Context for named parameters (e.g., :paramName).
 *
 * @param params Map of parameter names to their expression values
 */
case class NamedParameterContext(params: Map[String, Expression]) extends ParameterContext

/**
 * Context for positional parameters (e.g., ?).
 *
 * @param params Sequence of expression values in order
 */
case class PositionalParameterContext(params: Seq[Expression]) extends ParameterContext

/**
 * Context that supports both named and positional parameters.
 * This is used by EXECUTE IMMEDIATE where the parameter type is determined by the inner query.
 *
 * @param args Expression values from USING clause (already evaluated and resolved to Literals)
 * @param paramNames Parameter names from USING clause (empty strings for positional)
 */
case class HybridParameterContext(
    args: Seq[Expression],
    paramNames: Seq[String]) extends ParameterContext
