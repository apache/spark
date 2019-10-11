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
 * A [[PropertyGraph]] is created from GraphElementFrames.
 *
 * A graph element is either a node or a relationship.
 * A GraphElementFrame wraps a DataFrame and describes how it maps to graph elements.
 *
 * @since 3.0.0
 */
abstract class GraphElementFrame {

  /**
   * Initial DataFrame that can still contain unmapped, arbitrarily ordered columns.
   *
   * @since 3.0.0
   */
  def df: Dataset[Row]

  /**
   * Name of the column that contains the graph element identifier.
   *
   * @since 3.0.0
   */
  def idColumn: String

  /**
   * Name of all columns that contain graph element identifiers.
   *
   * @since 3.0.0
   */
  def idColumns: Seq[String] = Seq(idColumn)

  /**
   * Mapping from graph element property keys to the columns that contain the corresponding property
   * values.
   *
   * @since 3.0.0
   */
  def properties: Map[String, String]

}
