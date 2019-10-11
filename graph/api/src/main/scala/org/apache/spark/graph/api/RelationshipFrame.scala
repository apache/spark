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

package org.apache.spark.graph.api

import org.apache.spark.sql.{Dataset, Row}

/**
 * Describes how to map a DataFrame to relationships.
 *
 * Each row in the DataFrame represents a relationship with the given relationship type.
 *
 * @param df               DataFrame containing a single relationship in each row
 * @param idColumn         column that contains the relationship identifier
 * @param sourceIdColumn   column that contains the source node identifier of the relationship
 * @param targetIdColumn   column that contains the target node identifier of the relationship
 * @param relationshipType relationship type that is assigned to all relationships
 * @param properties       mapping from property keys to corresponding columns
 * @since 3.0.0
 */
case class RelationshipFrame private[graph] (
    df: Dataset[Row],
    idColumn: String,
    sourceIdColumn: String,
    targetIdColumn: String,
    relationshipType: String,
    properties: Map[String, String])
  extends GraphElementFrame {

  override def idColumns: Seq[String] = Seq(idColumn, sourceIdColumn, targetIdColumn)

}
