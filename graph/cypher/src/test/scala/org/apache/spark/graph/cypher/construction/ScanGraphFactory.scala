package org.apache.spark.graph.cypher.construction

import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.graph.cypher.conversions.TemporalConversions._
import org.apache.spark.graph.cypher.conversions.TypeConversions._
import org.apache.spark.graph.cypher.{SparkCypherSession, SparkNodeTable, SparkRelationshipTable}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.temporal.Duration
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.graph.ScanGraph
import org.opencypher.okapi.testing.propertygraph.{CreateGraphFactory, CypherTestGraphFactory, InMemoryTestGraph}

import scala.collection.JavaConverters._

object ScanGraphFactory extends CypherTestGraphFactory[SparkCypherSession] {

  def encodeIdColumns(df: DataFrame, mapping: EntityMapping): DataFrame = {

    val idCols = mapping.idKeys.map { columnName =>
      val dataType = df.schema.fields(df.schema.fieldIndex(columnName)).dataType
      dataType match {
        case LongType => df.col(columnName).cast(StringType).cast(BinaryType)
        case IntegerType => df.col(columnName).cast(StringType).cast(BinaryType)
        case StringType => df.col(columnName).cast(BinaryType)
        case BinaryType => df.col(columnName)
        case unsupportedType => throw IllegalArgumentException(
          expected = s"Column `$columnName` should have a valid identifier data type, such as [`$BinaryType`, `$StringType`, `$LongType`, `$IntegerType`]",
          actual = s"Unsupported column type `$unsupportedType`"
        )
      }
    }
    val remainingCols = mapping.allSourceKeys.filterNot(mapping.idKeys.contains).map(df.col)
    val colsToSelect = idCols ++ remainingCols
    df.select(colsToSelect: _*)
  }


  def initGraph(createQuery: String)
    (implicit sparkCypher: SparkCypherSession): RelationalCypherGraph[DataFrameTable] = {
    apply(CreateGraphFactory(createQuery))
  }

  val tableEntityIdKey = "___id"
  val tableEntityStartNodeKey = "___source"
  val tableEntityEndNodeKey = "___target"

  override def apply(propertyGraph: InMemoryTestGraph)
    (implicit sparkCypher: SparkCypherSession): ScanGraph[DataFrameTable] = {
    val schema = computeSchema(propertyGraph)

    val nodeScans = schema.labelCombinations.combos.map { labels =>
      val propKeys = schema.nodePropertyKeys(labels)

      val idStructField = Seq(StructField(tableEntityIdKey, LongType, nullable = false))
      val structType = StructType(idStructField ++ getPropertyStructFields(propKeys))

      val header = Seq(tableEntityIdKey) ++ propKeys.keys
      val rows = propertyGraph.nodes
        .filter(_.labels == labels)
        .map { node =>
          val propertyValues = propKeys.map(key =>
            node.properties.unwrap.get(key._1) match {
              case Some(date: LocalDate) => java.sql.Date.valueOf(date)
              case Some(localDateTime: LocalDateTime) => java.sql.Timestamp.valueOf(localDateTime)
              case Some(dur: Duration) => dur.toCalendarInterval
              case Some(other) => other
              case None => null
            }
          )
          Row.fromSeq(Seq(node.id) ++ propertyValues)
        }

      val records = sparkCypher.sparkSession.createDataFrame(rows.asJava, structType).toDF(header: _*)

      val nodeMapping = NodeMapping
        .on(tableEntityIdKey)
        .withImpliedLabels(labels.toSeq: _*)
        .withPropertyKeys(propKeys.keys.toSeq: _*)

      val encodedRecords = encodeIdColumns(records, nodeMapping)

      SparkNodeTable(nodeMapping, encodedRecords)
    }

    val relScans = schema.relationshipTypes.map { relType =>
      val propKeys = schema.relationshipPropertyKeys(relType)

      val idStructFields = Seq(
        StructField(tableEntityIdKey, LongType, nullable = false),
        StructField(tableEntityStartNodeKey, LongType, nullable = false),
        StructField(tableEntityEndNodeKey, LongType, nullable = false))
      val structType = StructType(idStructFields ++ getPropertyStructFields(propKeys))

      val header = Seq(tableEntityIdKey, tableEntityStartNodeKey, tableEntityEndNodeKey) ++ propKeys.keys
      val rows = propertyGraph.relationships
        .filter(_.relType == relType)
        .map { rel =>
          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
          Row.fromSeq(Seq(rel.id, rel.startId, rel.endId) ++ propertyValues)
        }

      val records = sparkCypher.sparkSession.createDataFrame(rows.asJava, structType).toDF(header: _*)

      val relationshipMapping = RelationshipMapping
        .on(tableEntityIdKey)
        .from(tableEntityStartNodeKey)
        .to(tableEntityEndNodeKey)
        .relType(relType)
        .withPropertyKeys(propKeys.keys.toSeq: _*)

      val encodedRecords = encodeIdColumns(records, relationshipMapping)

      SparkRelationshipTable(relationshipMapping, encodedRecords)
    }

    new ScanGraph(nodeScans.toSeq ++ relScans, schema)
  }

  override def name: String = getClass.getSimpleName

  protected def getPropertyStructFields(propKeys: PropertyKeys): Seq[StructField] = {
    propKeys.foldLeft(Seq.empty[StructField]) {
      case (fields, key) => fields :+ StructField(key._1, key._2.getSparkType, key._2.isNullable)
    }
  }
}
