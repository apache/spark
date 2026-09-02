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

package org.apache.spark.sql.connector.catalog

import java.util

import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.{SortOrder, Transform}

class InMemoryTableWithV2FilterCatalog extends InMemoryTableCatalog {
  // scalastyle:off argcount
  override protected def newInMemoryTable(
      name: String,
      columns: Array[Column],
      partitioning: Array[Transform],
      properties: util.Map[String, String],
      constraints: Array[Constraint],
      distribution: Distribution,
      ordering: Array[SortOrder],
      requiredNumPartitions: Option[Int],
      advisoryPartitionSize: Option[Long],
      distributionStrictlyRequired: Boolean,
      numRowsPerSplit: Int,
      id: String): InMemoryBaseTable = {
    // scalastyle:on argcount
    new InMemoryTableWithV2Filter(
      name, columns, partitioning, properties, constraints, distribution, ordering,
      requiredNumPartitions, advisoryPartitionSize, distributionStrictlyRequired, numRowsPerSplit)
  }
}
