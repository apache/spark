package org.apache.spark.graph.cypher.io

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.graph.api.io.ReaderConfig
import org.apache.spark.graph.api.{NodeFrame, RelationshipFrame}
import org.apache.spark.graph.cypher.SparkGraphDirectoryStructure
import org.apache.spark.graph.cypher.SparkGraphDirectoryStructure._
import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.graph.cypher.conversions.StringEncodingUtilities._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.okapi.api.graph.{SourceEndNodeKey, SourceIdKey, SourceStartNodeKey}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.{Property, Var}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph

object ReadWriteGraph {

  case class GraphImporter(sparkSession: SparkSession, config: ReaderConfig) {

    import org.apache.spark.graph.cypher.util.HadoopFSUtils._

    val directoryStructure: SparkGraphDirectoryStructure = SparkGraphDirectoryStructure(config.path)
    val fs: FileSystem = FileSystem.get(new URI(config.path), sparkSession.sparkContext.hadoopConfiguration)

    def nodeFrames: Seq[NodeFrame] = {
      val nodeLabelComboDirectories = fs.listDirectories(directoryStructure.pathToNodeDirectory)
      val labelCombos = nodeLabelComboDirectories.map(_.toLabelCombo)
      labelCombos.map { combo =>
        val df = sparkSession.read.format(config.source).load(directoryStructure.pathToNodeTable(combo))
        val propertyMappings = df.columns.collect {
          case colName if colName.isPropertyColumnName => colName.toProperty -> colName
        }.toMap
        NodeFrame(
          df,
          SourceIdKey.name,
          combo,
          propertyMappings)
      }
    }

    def relationshipFrames: Seq[RelationshipFrame] = {
      val relTypeDirectories = fs.listDirectories(directoryStructure.pathToRelationshipDirectory)
      val relTypes = relTypeDirectories.map(_.toRelationshipType)
      relTypes.map { relType =>
        val df = sparkSession.read.format(config.source).load(directoryStructure.pathToRelationshipTable(relType))
        val propertyMappings = df.columns.collect {
          case colName if colName.isPropertyColumnName => colName.toProperty -> colName
        }.toMap
        RelationshipFrame(
          df,
          SourceIdKey.name,
          SourceStartNodeKey.name,
          SourceEndNodeKey.name,
          relType,
          propertyMappings)
      }
    }

    def close(): Unit = {
      fs.close()
    }

  }

  implicit class GraphExport(graph: RelationalCypherGraph[DataFrameTable]) {

    def canonicalNodeTable(labels: Set[String]): DataFrame = {
      val ct = CTNode(labels)
      val v = Var("n")(ct)
      val nodeRecords = graph.nodes(v.name, ct, exactLabelMatch = true)
      val header = nodeRecords.header

      val idRenaming = header.column(v) -> SourceIdKey.name
      val properties: Set[Property] = header.propertiesFor(v)
      val propertyRenames = properties.map { p => header.column(p) -> p.key.name.toPropertyColumnName }

      val selectColumns = (idRenaming :: propertyRenames.toList.sortBy { case (_, newName) => newName }).map {
        case (oldName, newName) => nodeRecords.table.df.col(oldName).as(newName)
      }

      nodeRecords.table.df.select(selectColumns: _*)
    }

    def canonicalRelationshipTable(relType: String): DataFrame = {
      val ct = CTRelationship(relType)
      val v = Var("r")(ct)
      val relRecords = graph.relationships(v.name, ct)
      val header = relRecords.header

      val idRenaming = header.column(v) -> SourceIdKey.name
      val sourceIdRenaming = header.column(header.startNodeFor(v)) -> SourceStartNodeKey.name
      val targetIdRenaming = header.column(header.endNodeFor(v)) -> SourceEndNodeKey.name
      val properties: Set[Property] = relRecords.header.propertiesFor(v)
      val propertyRenames = properties.map { p => relRecords.header.column(p) -> p.key.name.toPropertyColumnName }

      val selectColumns = (idRenaming :: sourceIdRenaming :: targetIdRenaming :: propertyRenames.toList.sortBy { case (_, newName) => newName }).map {
        case (oldName, newName) => relRecords.table.df.col(oldName).as(newName)
      }

      relRecords.table.df.select(selectColumns: _*)
    }

  }

}
