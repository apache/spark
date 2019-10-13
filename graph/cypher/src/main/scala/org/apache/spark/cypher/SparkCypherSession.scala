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

import org.apache.spark.cypher.SparkCypherSession._
import org.apache.spark.cypher.SparkTable.DataFrameTable
import org.apache.spark.cypher.adapters.RelationalGraphAdapter
import org.apache.spark.cypher.conversions.GraphElementFrameConversions.normalizeDf
import org.apache.spark.cypher.io.SparkCypherPropertyGraphReader
import org.apache.spark.graph.api._
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.okapi.relational.api.table.RelationalElementTableFactory

object SparkCypherSession {
  def create(implicit sparkSession: SparkSession): CypherSession = new SparkCypherSession(sparkSession)

  private[spark] def createInternal(implicit sparkSession: SparkSession): SparkCypherSession = new SparkCypherSession(sparkSession)

  private[cypher] def toRelationalGraph(graph: PropertyGraph): RelationalCypherGraph[DataFrameTable] = {
    graph match {
      case adapter: RelationalGraphAdapter => adapter.graph
      case other => throw IllegalArgumentException(
        expected = "A graph that has been created by `SparkCypherSession.createGraph`",
        actual = other.getClass.getSimpleName
      )
    }
  }
}

/**
  * Default [[CypherSession]] implementation.
  *
  * This class is the main entry point for working with the spark-cypher module.
  * It wraps a [[SparkSession]] and allows to run Cypher queries over graphs represented as [[org.apache.spark.sql.Dataset]]s.
  */
private[spark] class SparkCypherSession(override val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] with CypherSession {

  override type Result = RelationalCypherResult[DataFrameTable]
  override type Records = SparkCypherRecords

  implicit def sparkCypherSession: SparkCypherSession = this

  override val records: SparkCypherRecordsFactory = SparkCypherRecordsFactory()

  override val graphs: RelationalCypherGraphFactory[DataFrameTable] = {
    new RelationalCypherGraphFactory[DataFrameTable]() {
      override implicit val session: RelationalCypherSession[DataFrameTable] = sparkCypherSession
    }
  }

  override def elementTables: RelationalElementTableFactory[DataFrameTable] = {
    throw UnsupportedOperationException("Graph construction with `CONSTRUCT` is not supported in Cypher 9")
  }

  override def createGraph(nodes: Array[NodeDataset], relationships: Array[RelationshipDataset]): PropertyGraph = {
    require(nodes.groupBy(_.labelSet).forall(_._2.length == 1),
      "There can be at most one NodeDataset per label set")
    require(relationships.groupBy(_.relationshipType).forall(_._2.length == 1),
      "There can be at most one RelationshipDataset per relationship type")

    val normalizedNodes = nodes.map(nf => nf.copy(ds = normalizeDf(nf)))
    val normalizedRelationships = relationships.map(rf => rf.copy(ds = normalizeDf(rf)))
    RelationalGraphAdapter(this, normalizedNodes, normalizedRelationships)
  }

  def cypher(graph: PropertyGraph, query: String): CypherResult = cypher(graph, query, Map.empty[String, Object])

  override def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult = {
    val relationalGraph = toRelationalGraph(graph)
    SparkCypherResult(relationalGraph.cypher(query, CypherMap(parameters.toSeq: _*)).records)
  }

  override def read(): SparkCypherPropertyGraphReader =
    new SparkCypherPropertyGraphReader(this)

}
