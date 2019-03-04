package org.apache.spark.graph.cypher

import org.apache.spark.graph.api._
import org.apache.spark.graph.api.io.{ReaderConfig, WriterConfig}
import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.graph.cypher.adapters.MappingAdapter._
import org.apache.spark.graph.cypher.adapters.RelationalGraphAdapter
import org.apache.spark.graph.cypher.io.ReadWriteGraph._
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory

object SparkCypherSession {
  def create(implicit sparkSession: SparkSession): CypherSession = new SparkCypherSession(sparkSession)

  private[spark] def createInternal(implicit sparkSession: SparkSession): SparkCypherSession = new SparkCypherSession(sparkSession)
}

/**
  * Default [[CypherSession]] implementation.
  *
  * This class is the main entry point for working with the spark-graph-cypher module.
  * It wraps a [[SparkSession]] and allows to run Cypher queries over graphs represented as [[org.apache.spark.sql.DataFrame]]s.
  */
private[spark] class SparkCypherSession(override val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] with CypherSession {

  override type Result = RelationalCypherResult[DataFrameTable]
  override type Records = SparkCypherRecords
  override type Graph = RelationalCypherGraph[DataFrameTable]

  implicit def sparkCypherSession: SparkCypherSession = this

  override val records: SparkCypherRecordsFactory = SparkCypherRecordsFactory()

  override val graphs: RelationalCypherGraphFactory[DataFrameTable] = {
    new RelationalCypherGraphFactory[DataFrameTable]() {
      override implicit val session: RelationalCypherSession[DataFrameTable] = sparkCypherSession
    }
  }

  override def entityTables: RelationalEntityTableFactory[DataFrameTable] = {
    throw UnsupportedOperationException("Graph construction with `CONSTRUCT` is not supported in Cypher 9")
  }

  override def createGraph(nodes: Seq[NodeFrame], relationships: Seq[RelationshipFrame] = Seq.empty): PropertyGraph = {
    require(nodes.nonEmpty, "Creating a graph requires at least one NodeDataFrame")
    val nodeTables = nodes.map { nodeDataFrame => SparkEntityTable(nodeDataFrame.toNodeMapping, nodeDataFrame.df) }
    val relTables = relationships.map { relDataFrame => SparkEntityTable(relDataFrame.toRelationshipMapping, relDataFrame.df) }

    RelationalGraphAdapter(this, graphs.create(nodeTables.head, nodeTables.tail ++ relTables: _*))
  }

  override def createGraph(result: CypherResult): PropertyGraph = {
    val sparkCypherResult = result match {
      case r: SparkCypherResult => r
      case other => throw IllegalArgumentException(
        expected = "A result that has been created by `SparkCypherSession.cypher`",
        actual = other.getClass.getSimpleName
      )
    }

    val entityVars = sparkCypherResult.relationalTable.header.entityVars
    val nodeVarNames = entityVars.collect { case v if v.cypherType.subTypeOf(CTNode).isTrue => v.name }
    val relVarNames = entityVars.collect { case v if v.cypherType.subTypeOf(CTRelationship).isTrue => v.name }

    val nodeFrames = nodeVarNames.flatMap(result.nodeFrames).toSeq
    val relFrames = relVarNames.flatMap(result.relationshipFrames).toSeq

    createGraph(nodeFrames, relFrames)
  }

  def cypher(graph: PropertyGraph, query: String): CypherResult = cypher(graph, query, Map.empty)

  override def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult = {
    val relationalGraph = toRelationalGraph(graph)
    SparkCypherResult(relationalGraph.cypher(query, CypherMap(parameters.toSeq: _*)).records, relationalGraph.schema)
  }

  override private[spark] def readGraph(config: ReaderConfig): PropertyGraph = {
    val graphImporter = GraphImporter(sparkSession, config)
    createGraph(graphImporter.nodeFrames, graphImporter.relationshipFrames)
  }

  override private[spark] def writeGraph(graph: PropertyGraph, config: WriterConfig): Unit = {
    val relationalGraph = toRelationalGraph(graph)
    val graphDirectoryStructure = SparkGraphDirectoryStructure(config.path)

    relationalGraph.schema.labelCombinations.combos.foreach { combo =>
      relationalGraph.canonicalNodeTable(combo)
        .write
        .format(config.source)
        .mode(config.mode)
        .save(graphDirectoryStructure.pathToNodeTable(combo))
    }
    relationalGraph.schema.relationshipTypes.foreach { relType =>
      relationalGraph.canonicalRelationshipTable(relType)
        .write
        .format(config.source)
        .mode(config.mode)
        .save(graphDirectoryStructure.pathToRelationshipTable(relType))
    }
  }

  private def toRelationalGraph(graph: PropertyGraph): RelationalCypherGraph[DataFrameTable] = {
    graph match {
      case RelationalGraphAdapter(_, relGraph) => relGraph
      case other => throw IllegalArgumentException(
        expected = "A graph that has been created by `SparkCypherSession.createGraph`",
        actual = other.getClass.getSimpleName
      )
    }
  }

}
