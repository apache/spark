package org.apache.spark.graph.cypher.conversions

import org.apache.spark.graph.cypher.conversions.TemporalConversions._
import org.apache.spark.graph.cypher.{SparkCypherNode, SparkCypherRelationship}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.unsafe.types.CalendarInterval
import org.opencypher.okapi.api.types.{CTList, CTMap, CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.{Expr, ListSegment, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader

// TODO: argument cannot be a Map due to Scala issue https://issues.scala-lang.org/browse/SI-7005
final case class RowConversion(exprToColumn: Seq[(Expr, String)]) extends (Row => CypherMap) {

  private val header = RecordHeader(exprToColumn.toMap)

  override def apply(row: Row): CypherMap = {
    val values = header.returnItems.map(r => r.name -> constructValue(row, r)).toSeq
    CypherMap(values: _*)
  }

  // TODO: Validate all column types. At the moment null values are cast to the expected type...
  private def constructValue(row: Row, v: Var): CypherValue = {
    v.cypherType.material match {
      case _: CTNode =>
        collectNode(row, v)

      case _: CTRelationship =>
        collectRel(row, v)

      case CTList(_) if !header.exprToColumn.contains(v) =>
        collectComplexList(row, v)

      case _ => constructFromExpression(row, v)
    }
  }

  private def constructFromExpression(row: Row, expr: Expr): CypherValue = {
    expr.cypherType.material match {
      case CTMap(inner) =>
        if (inner.isEmpty) {
          CypherMap()
        } else {
          val innerRow = row.getAs[GenericRowWithSchema](header.column(expr))
          innerRow match {
            case _: GenericRowWithSchema =>
              innerRow.schema.fieldNames.map { field =>
                field -> CypherValue(innerRow.getAs[Any](field))
              }.toMap
            case null => null
          }
        }

      case _ =>
        val raw = row.getAs[Any](header.column(expr))
        raw match {
          case interval: CalendarInterval => interval.toDuration
          case other => CypherValue(other)
        }
    }
  }

  private def collectNode(row: Row, v: Var): CypherValue = {
    val idValue = row.getAs[Any](header.column(v))
    idValue match {
      case null => CypherNull
      case id: Array[_] =>

        val labels = header
          .labelsFor(v)
          .map { l => l.label.name -> row.getAs[Boolean](header.column(l)) }
          .collect { case (name, true) => name }

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> constructFromExpression(row, p) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        SparkCypherNode(id.asInstanceOf[Array[Byte]], labels, properties)
      case invalidID => throw UnsupportedOperationException(s"CAPSNode ID has to be a CAPSId instead of ${invalidID.getClass}")
    }
  }

  private def collectRel(row: Row, v: Var): CypherValue = {
    val idValue = row.getAs[Any](header.column(v))
    idValue match {
      case null => CypherNull
      case id: Array[_] =>
        val source = row.getAs[Array[_]](header.column(header.startNodeFor(v)))
        val target = row.getAs[Array[_]](header.column(header.endNodeFor(v)))

        val relType = header
          .typesFor(v)
          .map { l => l.relType.name -> row.getAs[Boolean](header.column(l)) }
          .collect { case (name, true) => name }
          .head

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> constructFromExpression(row, p) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        SparkCypherRelationship(
          id.asInstanceOf[Array[Byte]],
          source.asInstanceOf[Array[Byte]],
          target.asInstanceOf[Array[Byte]],
          relType,
          properties)
      case invalidID => throw UnsupportedOperationException(s"CAPSRelationship ID has to be a Long instead of ${invalidID.getClass}")
    }
  }

  private def collectComplexList(row: Row, expr: Var): CypherList = {
    val elements = header.ownedBy(expr).collect {
      case p: ListSegment => p
    }.toSeq.sortBy(_.index)

    val values = elements
      .map(constructValue(row, _))
      .filter {
        case CypherNull => false
        case _ => true
      }

    CypherList(values)
  }
}

