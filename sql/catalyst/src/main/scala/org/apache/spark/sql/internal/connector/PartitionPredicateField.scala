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

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.types.StructField

/**
 * Metadata for one identity partition field.
 *
 * @param structField the Catalyst field describing the partition column (name, type,
 *                    nullability). For nested columns the name is the dotted path
 *                    (e.g. `"s.tz"`).
 * @param identityRef the [[NamedReference]] from the transform target column
 *                    (e.g. `FieldReference(Seq("s", "tz"))`).
 */
case class PartitionPredicateField(structField: StructField, identityRef: NamedReference)
