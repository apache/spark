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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A TableFormat enables the creation and loading of a logical table at a physical TableLocation.
 *
 * Concrete classes will be instantiated with a very simple form of dependency injection;
 * a SQLContext (or HiveContext) and Hadoop Configuration are available. Only one constructor
 * is allowed.
 */
abstract class TableFormat {
  /**
   * Constructs a new, empty Relation with the given schema. It is the responsibility of
   * the caller to ensure that the given location is either empty, or else that it is OK to
   * overwrite any data already there.
   * @param location Description of the physical location where the table data should be put
   * @param schema Schema of the new relation
   * @return Newly constructed Relation (e.g., a ParquetRelation)
   */
  def createEmptyRelation(location: TableLocation, schema: Seq[Attribute]): LogicalPlan

  /**
   * Loads an existing Relation from the given location. It is only valid to call this
   * after calling createEmptyRelation() for the same location.
   * @param location Description of the physical location of the table data
   * @return Relation over the given data (e.g., a ParquetRelation)
   */
  def loadExistingRelation(location: TableLocation): LogicalPlan
}

/**
 * Describes the physical location of a table.
 */
private[sql] trait TableLocation extends Serializable
