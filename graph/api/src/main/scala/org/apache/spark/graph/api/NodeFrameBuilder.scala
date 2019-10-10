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

import org.apache.spark.sql.DataFrame

/**
 * Interface used to build a [[NodeFrame]].
 *
 * @param df DataFrame containing a single node in each row
 * @since 3.0.0
 */
final class NodeFrameBuilder(var df: DataFrame) {

  private var idColumn: String = CypherSession.ID_COLUMN
  private var labelSet: Set[String] = Set.empty
  private var properties: Map[String, String] = Map.empty

  /**
   * @param idColumn column that contains the node identifier
   * @since 3.0.0
   */
  def idColumn(idColumn: String): NodeFrameBuilder = {
    if (idColumn.isEmpty) {
      throw new IllegalArgumentException("idColumn must not be empty")
    }
    this.idColumn = idColumn;
    this
  }

  /**
   * @param labelSet labels that are assigned to all nodes
   * @since 3.0.0
   */
  def labelSet(labelSet: Array[String]): NodeFrameBuilder = {
    this.labelSet = labelSet.toSet
    this
  }

  /**
   * @param properties mapping from property keys to corresponding columns
   * @since 3.0.0
   */
  def properties(properties: Map[String, String]): NodeFrameBuilder = {
    this.properties = properties
    this
  }

  /**
   * @param properties mapping from property keys to corresponding columns
   * @since 3.0.0
   */
  def properties(properties: java.util.Map[String, String]): NodeFrameBuilder = {
    this.properties = properties.asScala.toMap
    this
  }

  /**
   * Creates a `NodeFrame` from the specified builder parameters.
   *
   * @since 3.0.0
   */
  def build(): NodeFrame = {
    NodeFrame(df, idColumn, labelSet, properties)
  }

}
