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
 * Exception thrown when the underlying parser meet a bad record and can't parse it.
 * @param record a function to return the record that cause the parser to fail
 * @param partialResults a function that returns an optional rows, which is the partial results of
 *                      parsing this bad record.
 * @param hasErrors a function that returns an optional booleans, which is used to know if a row
 *                  encounters error when parsing it.
 * @param cause the actual exception about why the record is bad and can't be parsed.
 */
case class BadRecordException(
    record: () => UTF8String,
    partialResults: () => Option[Seq[InternalRow]],
    hasErrors: () => Option[Seq[Boolean]],
    cause: Throwable) extends Exception(cause)
