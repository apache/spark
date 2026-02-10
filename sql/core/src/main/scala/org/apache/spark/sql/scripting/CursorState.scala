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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

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
 * Cursor has been opened and result iterator has been created.
 *
 * CRITICAL: The iterator is created at OPEN time (not first FETCH) to ensure snapshot
 * semantics. When executeToIterator() is called, Spark performs file discovery and
 * captures Delta snapshots. This is the ONLY way to lock in the data snapshot at OPEN time.
 *
 * The iterator is lazy/incremental - it doesn't materialize all results, but it does
 * lock in which files/versions will be read.
 *
 * @param resultIterator Iterator created at OPEN time (snapshot captured)
 * @param outputSchema The output attributes (needed for type checking in FETCH)
 */
case class CursorOpened(
    resultIterator: Iterator[InternalRow],
    outputSchema: Seq[Attribute]) extends CursorState

/**
 * Cursor is being fetched from - same as CursorOpened but marks that fetching has begun.
 * We keep this separate state for consistency and potential future use.
 */
case class CursorFetching(
    resultIterator: Iterator[InternalRow],
    outputSchema: Seq[Attribute]) extends CursorState

/**
 * Cursor has been closed and resources released.
 */
case object CursorClosed extends CursorState
