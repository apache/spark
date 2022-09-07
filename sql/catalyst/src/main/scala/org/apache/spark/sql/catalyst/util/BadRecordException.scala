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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

/**
 * Exception thrown when the underlying parser returns a partial result of parsing.
 * @param partialResult the partial result of parsing a bad record.
 * @param cause the actual exception about why the parser cannot return full result.
 */
case class PartialResultException(
     partialResult: InternalRow,
     cause: Throwable)
  extends Exception(cause)

/**
 * Exception thrown when the underlying parser meet a bad record and can't parse it.
 * @param record a function to return the record that cause the parser to fail
 * @param partialResult a function that returns an optional row, which is the partial result of
 *                      parsing this bad record.
 * @param cause the actual exception about why the record is bad and can't be parsed.
 */
case class BadRecordException(
    @transient record: () => UTF8String,
    @transient partialResult: () => Option[InternalRow],
    cause: Throwable) extends Exception(cause)
