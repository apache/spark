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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.{Dataset, Row}

/**
 * A builder for [[RelationshipDataset]].
 *
 * @param ds Dataset containing a single relationship in each row
 * @since 3.0.0
 */
@Evolving
final class RelationshipDatasetBuilder(val ds: Dataset[Row]) {

  private var idColumn: String = CypherSession.ID_COLUMN
  private var sourceIdColumn: String = CypherSession.SOURCE_ID_COLUMN
  private var targetIdColumn: String = CypherSession.TARGET_ID_COLUMN
  private var maybeRelationshipType: Option[String] = Option.empty
  private var properties: Map[String, String] = Map.empty

  /**
   * @param idColumn column that contains the relationship identifier
   * @since 3.0.0
   */
  def idColumn(idColumn: String): RelationshipDatasetBuilder = {
    if (idColumn.isEmpty) {
      throw new IllegalArgumentException("idColumn must not be empty")
    }
    this.idColumn = idColumn;
    this
  }

  /**
   * @param sourceIdColumn column that contains the source node identifier of the relationship
   * @since 3.0.0
   */
  def sourceIdColumn(sourceIdColumn: String): RelationshipDatasetBuilder = {
    if (sourceIdColumn.isEmpty) {
      throw new IllegalArgumentException("sourceIdColumn must not be empty")
    }
    this.sourceIdColumn = sourceIdColumn;
    this
  }

  /**
   * @param targetIdColumn column that contains the target node identifier of the relationship
   * @since 3.0.0
   */
  def targetIdColumn(targetIdColumn: String): RelationshipDatasetBuilder = {
    if (targetIdColumn.isEmpty) {
      throw new IllegalArgumentException("targetIdColumn must not be empty")
    }
    this.targetIdColumn = targetIdColumn;
    this
  }

  /**
   * @param relationshipType relationship type that is assigned to all relationships
   * @since 3.0.0
   */
  def relationshipType(relationshipType: String): RelationshipDatasetBuilder = {
    if (relationshipType.isEmpty) {
      throw new IllegalArgumentException("Relationship type must not be empty")
    }
    this.maybeRelationshipType = Some(relationshipType)
    this
  }

  /**
   * @param properties mapping from property keys to corresponding columns
   * @since 3.0.0
   */
  def properties(properties: Map[String, String]): RelationshipDatasetBuilder = {
    this.properties = properties
    this
  }

  /**
   * @param properties mapping from property keys to corresponding columns
   * @since 3.0.0
   */
  def properties(properties: java.util.Map[String, String]): RelationshipDatasetBuilder = {
    this.properties = properties.asScala.toMap
    this
  }

  /**
   * Creates a [[RelationshipDataset]] from the specified builder parameters.
   *
   * @since 3.0.0
   */
  def build(): RelationshipDataset = {
    maybeRelationshipType match {
      case Some(relType) =>
        RelationshipDataset(ds, idColumn, sourceIdColumn, targetIdColumn, relType, properties)
      case None => throw new IllegalArgumentException("Relationship type must be set.")
    }
  }
}
