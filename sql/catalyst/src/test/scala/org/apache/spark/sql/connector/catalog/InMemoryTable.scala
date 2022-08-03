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

import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{SortOrder, Transform}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * A simple in-memory table. Rows are stored as a buffered group produced by each output task.
 */
class InMemoryTable(
    val _name: String,
    val _schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String],
    val _distribution: Distribution = Distributions.unspecified(),
    val _ordering: Array[SortOrder] = Array.empty,
    val _numPartitions: Option[Int] = None,
    val _isDistributionStrictlyRequired: Boolean = true)
  extends InMemoryBaseTable(_name, _schema, partitioning, properties, _distribution,
    _ordering, _numPartitions, _isDistributionStrictlyRequired) with SupportsDelete {

  override def canDeleteWhere(filters: Array[Filter]): Boolean = {
    InMemoryBaseTable.supportsFilters(filters)
  }

  override def deleteWhere(filters: Array[Filter]): Unit = dataMap.synchronized {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    dataMap --= InMemoryBaseTable.filtersToKeys(dataMap.keys, partCols.map(_.toSeq.quoted), filters)
  }

  override def withData(data: Array[BufferedRows]): InMemoryTable = {
    withData(data, schema)
  }

  override def withData(
      data: Array[BufferedRows],
      writeSchema: StructType): InMemoryTable = dataMap.synchronized {
    data.foreach(_.rows.foreach { row =>
      val key = getKey(row, writeSchema)
      dataMap += dataMap.get(key)
        .map(key -> _.withRow(row))
        .getOrElse(key -> new BufferedRows(key).withRow(row))
      addPartitionKey(key)
    })
    this
  }
}
