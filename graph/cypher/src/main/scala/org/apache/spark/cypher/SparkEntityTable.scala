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
 *
 */

package org.apache.spark.cypher

import org.apache.spark.cypher.SparkTable.DataFrameTable
import org.opencypher.okapi.api.io.conversion.ElementMapping
import org.opencypher.okapi.relational.api.io.ElementTable

case class SparkEntityTable(
  override val mapping: ElementMapping,
  override val table: DataFrameTable
) extends ElementTable[DataFrameTable] with RecordBehaviour {

  override type Records = SparkEntityTable

  private[spark] def records(implicit cypherEngine: SparkCypherSession): SparkCypherRecords = cypherEngine.records.fromElementTable(entityTable = this)

  override def cache(): SparkEntityTable = {
    table.cache()
    this
  }
}
