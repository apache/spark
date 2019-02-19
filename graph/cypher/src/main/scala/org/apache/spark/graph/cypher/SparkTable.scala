package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.conversions.ExprConversions._
import org.apache.spark.graph.cypher.conversions.TypeConversions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.planning.{JoinType, Order}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object SparkTable {

  implicit class DataFrameTable(val df: DataFrame) extends Table[DataFrameTable] {

    override def columnType: Map[String, CypherType] = physicalColumns.map(c => c -> df.cypherTypeForColumn(c)).toMap

    override def physicalColumns: Seq[String] = df.columns

    override def select(cols: String*): DataFrameTable = {
      if (df.columns.toSeq == cols) {
        df
      } else {
        df.select(cols.map(df.col): _*)
      }
    }

    override def withColumns(columns: (Expr, String)*)
      (implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {
      val initialColumnNameToColumn: Map[String, Column] = df.columns.map(c => c -> df.col(c)).toMap
      val updatedColumns = columns.foldLeft(initialColumnNameToColumn) { case (columnMap, (expr, columnName)) =>
        val column = expr.asSparkSQLExpr(header, df, parameters).as(columnName)
        columnMap + (columnName -> column)
      }
      // TODO: Re-enable this check as soon as types (and their nullability) are correctly inferred in typing phase
      //      if (!expr.cypherType.isNullable) {
      //        withColumn.setNonNullable(column)
      //      } else {
      //        withColumn
      //      }
      val existingColumnNames = df.columns
      // Preserve order of existing columns
      val columnsForSelect = existingColumnNames.map(updatedColumns) ++
        updatedColumns.filterKeys(!existingColumnNames.contains(_)).values

      df.select(columnsForSelect: _*)
    }

    override def withColumnsRenamed(columnRenamings: Map[String, String]): DataFrameTable = {
      df.safeRenameColumns(columnRenamings)
    }

    override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): DataFrameTable = ???
    override def drop(cols: String*): DataFrameTable = ???
    override def join(
      other: DataFrameTable,
      joinType: JoinType,
      joinCols: (String, String)*
    ): DataFrameTable = ???
    override def unionAll(other: DataFrameTable): DataFrameTable = ???
    override def orderBy(sortItems: (Expr, Order)*)
      (
        implicit header: RecordHeader,
        parameters: CypherValue.CypherMap
      ): DataFrameTable = ???
    override def skip(n: Long): DataFrameTable = ???
    override def limit(n: Long): DataFrameTable = ???
    override def distinct: DataFrameTable = ???
    override def group(
      by: Set[Var],
      aggregations: Set[(Aggregator, (String, CypherType))]
    )
      (
        implicit header: RecordHeader,
        parameters: CypherValue.CypherMap
      ): DataFrameTable = ???


    override def show(rows: Int): Unit = ???

    override def columnsFor(returnItem: String): Set[String] = ???
    override def rows: Iterator[String => CypherValue.CypherValue] = ???
    override def size: Long = ???
  }

  implicit class DataFrameOps(val df: DataFrame) extends AnyVal {

    def cypherTypeForColumn(columnName: String): CypherType = {
      val structField = structFieldForColumn(columnName)
      val compatibleCypherType = structField.dataType.cypherCompatibleDataType.flatMap(_.toCypherType(structField.nullable))
      compatibleCypherType.getOrElse(
        throw IllegalArgumentException("a supported Spark DataType that can be converted to CypherType", structField.dataType))
    }

    def structFieldForColumn(columnName: String): StructField = {
      if (df.schema.fieldIndex(columnName) < 0) {
        throw IllegalArgumentException(s"column with name $columnName", s"columns with names ${df.columns.mkString("[", ", ", "]")}")
      }
      df.schema.fields(df.schema.fieldIndex(columnName))
    }

    def safeRenameColumns(renamings: Map[String, String]): DataFrame = {
      if (renamings.isEmpty || renamings.forall { case (oldColumn, newColumn) => oldColumn == newColumn }) {
        df
      } else {
        renamings.foreach { case (oldName, newName) => require(!df.columns.contains(newName),
          s"Cannot rename column `$oldName` to `$newName`. A column with name `$newName` exists already.")
        }
        val newColumns = df.columns.map {
          case col if renamings.contains(col) => renamings(col)
          case col => col
        }
        df.toDF(newColumns: _*)
      }
    }

  }

}
