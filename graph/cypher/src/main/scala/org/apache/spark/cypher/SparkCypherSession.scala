package org.apache.spark.cypher

import org.apache.spark.cypher.SparkTable.DataFrameTable
import org.apache.spark.cypher.adapters.RelationalGraphAdapter
import org.apache.spark.cypher.io.ReadWriteGraph._
import org.apache.spark.graph.api._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
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
  * This class is the main entry point for working with the spark-cypher module.
  * It wraps a [[SparkSession]] and allows to run Cypher queries over graphs represented as [[org.apache.spark.sql.DataFrame]]s.
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

  override def entityTables: RelationalEntityTableFactory[DataFrameTable] = {
    throw UnsupportedOperationException("Graph construction with `CONSTRUCT` is not supported in Cypher 9")
  }

  override def createGraph(nodes: Seq[NodeFrame], relationships: Seq[RelationshipFrame] = Seq.empty): PropertyGraph = {
    RelationalGraphAdapter(this, nodes, relationships)
  }

  override def createGraph(nodes: DataFrame, relationships: DataFrame): PropertyGraph = {
    val idColumn = "$ID"
    val sourceIdColumn = "$SOURCE_ID"
    val targetIdColumn = "$TARGET_ID"

    val labelColumns = nodes.columns.filter(_.startsWith(":")).toSet
    val nodeProperties = (nodes.columns.toSet - idColumn -- labelColumns).map(col => col -> col).toMap

    val trueLit = functions.lit(true)
    val falseLit = functions.lit(false)

    // TODO: add empty set
    val nodeFrames = labelColumns.subsets().map { labelSet =>
      val predicate = labelColumns.map {
        case labelColumn if labelSet.contains(labelColumn) => nodes.col(labelColumn) === trueLit
        case labelColumn => nodes.col(labelColumn) === falseLit
      }.reduce(_ && _)

      NodeFrame(nodes.filter(predicate), idColumn, labelSet.map(_.substring(1)), nodeProperties)
    }

    val relTypeColumns = relationships.columns.filter(_.startsWith(":")).toSet
    val relProperties = (relationships.columns.toSet - idColumn - sourceIdColumn - targetIdColumn -- relTypeColumns).map(col => col -> col).toMap
    val relFrames = relTypeColumns.map { relTypeColumn =>
      val predicate = relationships.col(relTypeColumn) === trueLit

      RelationshipFrame(relationships.filter(predicate), idColumn, sourceIdColumn, targetIdColumn, relTypeColumn.substring(1), relProperties)
    }

    createGraph(nodeFrames.toSeq, relFrames.toSeq)
  }

  def cypher(graph: PropertyGraph, query: String): CypherResult = cypher(graph, query, Map.empty)

  override def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult = {
    val relationalGraph = toRelationalGraph(graph)
    SparkCypherResult(relationalGraph.cypher(query, CypherMap(parameters.toSeq: _*)).records)
  }

  private val DEFAULT_FORMAT = "parquet"

  override def load(path: String): PropertyGraph = {
    val graphImporter = GraphImporter(sparkSession, path, DEFAULT_FORMAT)
    createGraph(graphImporter.nodeFrames, graphImporter.relationshipFrames)
  }

  override private[spark] def save(graph: PropertyGraph, path: String, saveMode: SaveMode): Unit = {
    val relationalGraph = toRelationalGraph(graph)
    val graphDirectoryStructure = SparkGraphDirectoryStructure(path)

    relationalGraph.schema.labelCombinations.combos.foreach { combo =>
      relationalGraph.canonicalNodeTable(combo)
        .write
        .format(DEFAULT_FORMAT)
        .mode(saveMode)
        .save(graphDirectoryStructure.pathToNodeTable(combo))
    }
    relationalGraph.schema.relationshipTypes.foreach { relType =>
      relationalGraph.canonicalRelationshipTable(relType)
        .write
        .format(DEFAULT_FORMAT)
        .mode(saveMode)
        .save(graphDirectoryStructure.pathToRelationshipTable(relType))
    }
  }

  private def toRelationalGraph(graph: PropertyGraph): RelationalCypherGraph[DataFrameTable] = {
    graph match {
      case adapter: RelationalGraphAdapter => adapter.graph
      case other => throw IllegalArgumentException(
        expected = "A graph that has been created by `SparkCypherSession.createGraph`",
        actual = other.getClass.getSimpleName
      )
    }
  }

}
