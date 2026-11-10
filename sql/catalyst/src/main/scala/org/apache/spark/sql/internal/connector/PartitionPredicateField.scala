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

package org.apache.spark.sql.internal.connector

import org.apache.spark.sql.catalyst.expressions.AttributeReference

/**
 * Metadata for one field of `Table.partitioning()`. A partition predicate is built over the
 * fields in partitioning order, so their ordinals match the partition key a connector passes to
 * `PartitionPredicate.eval`.
 *
 * @param fieldNames  the multi-part field name from the table's partitioning
 *                    (e.g. `Seq("s", "tz")`) for an identity transform, or the transform's
 *                    description (e.g. `Seq("bucket(4, id)")`) otherwise.
 * @param attrRef  the [[AttributeReference]] a filter can reference, for an identity transform.
 *                 Created from the resolved partition field so it carries the flattened dotted
 *                 name (e.g. `"s.tz"`) for nested fields. None for any other transform: for now
 *                 Spark does not evaluate a filter against its partition value, so no filter
 *                 references it, but the field keeps its ordinal.
 */
case class PartitionPredicateField(
    fieldNames: Seq[String],
    attrRef: Option[AttributeReference])
