package org.apache.spark.graph.cypher

import org.apache.spark.graph.api._
import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.graph.cypher.adapters.MappingAdapter._
import org.apache.spark.graph.cypher.adapters.RelationalGraphAdapter
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory

object SparkCypherSession {
  def create(implicit sparkSession: SparkSession): SparkCypherSession = new SparkCypherSession(sparkSession)
}

/**
  * Default [[CypherSession]] implementation.
  *
  * This class is the main entry point for working with the spark-graph-cypher module.
  * It wraps a [[SparkSession]] and allows to run Cypher queries over graphs represented as [[org.apache.spark.sql.DataFrame]]s.
  */
class SparkCypherSession(override val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] with CypherSession {

  override type Result = RelationalCypherResult[DataFrameTable]
  override type Records = SparkCypherRecords
  override type Graph = RelationalCypherGraph[DataFrameTable]

  implicit def sparkCypherSession: SparkCypherSession = this

  override val records: SparkCypherRecordsFactory = SparkCypherRecordsFactory()

  override val graphs: RelationalCypherGraphFactory[DataFrameTable] = new RelationalCypherGraphFactory[DataFrameTable]() {
    override implicit val session: RelationalCypherSession[DataFrameTable] = sparkCypherSession
  }

  override def entityTables: RelationalEntityTableFactory[DataFrameTable] = ???

  override def createGraph(nodes: Seq[NodeFrame], relationships: Seq[RelationshipFrame]): PropertyGraph = {
    require(nodes.nonEmpty, "Creating a graph requires at least one NodeDataFrame")
    val nodeTables = nodes.map { nodeDataFrame => SparkEntityTable(nodeDataFrame.toNodeMapping, nodeDataFrame.df) }
    val relTables = relationships.map { relDataFrame => SparkEntityTable(relDataFrame.toRelationshipMapping, relDataFrame.df) }

    RelationalGraphAdapter(this, graphs.create(nodeTables.head, nodeTables.tail ++ relTables: _*))
  }

  def cypher(graph: PropertyGraph, query: String): CypherResult = cypher(graph, query, Map.empty)

  override def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult = {
    val relationalGraph = graph match {
      case RelationalGraphAdapter(_, relGraph) => relGraph
      case other => throw IllegalArgumentException(
        expected = "A graph that has been created by `SparkCypherSession.createGraph`",
        actual = other.getClass.getSimpleName
      )
    }

    SparkCypherResult(relationalGraph.cypher(query, CypherMap(parameters.toSeq: _*)).records, relationalGraph.schema)
  }
}
