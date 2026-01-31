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

package org.apache.spark.sql.scripting

/**
 * Sealed trait representing the lifecycle state of a cursor.
 * State transitions:
 *   Declared -> Opened -> Fetching -> Closed
 */
sealed trait CursorState

/**
 * Cursor has been declared but not yet opened.
 * This is the initial state after DECLARE CURSOR.
 * The query is not parsed or analyzed until OPEN time.
 */
case object CursorDeclared extends CursorState

/**
 * Cursor has been opened and query has been parsed/analyzed.
 * For parameterized cursors, the query here has parameters substituted.
 *
 * @param analyzedQuery The analyzed logical plan with parameters bound
 */
case class CursorOpened(
    analyzedQuery: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan) extends CursorState

/**
 * Cursor is being fetched from - result iterator is active.
 * Uses executeToIterator() to avoid loading all data into memory at once.
 * Uses InternalRow format to avoid unnecessary conversions.
 *
 * @param analyzedQuery The analyzed logical plan
 * @param resultIterator Iterator over InternalRow results
 */
case class CursorFetching(
    analyzedQuery: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
    resultIterator: Iterator[org.apache.spark.sql.catalyst.InternalRow]) extends CursorState

/**
 * Cursor has been closed and resources released.
 */
case object CursorClosed extends CursorState
