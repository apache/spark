package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.conversions.ExprConversions._
import org.apache.spark.graph.cypher.conversions.TypeConversions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, functions}
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.planning._
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.collection.JavaConverters._

object SparkTable {

  implicit class DataFrameTable(val df: DataFrame) extends Table[DataFrameTable] {

    private case class EmptyRow()

    override def physicalColumns: Seq[String] = df.columns

    override def columnType: Map[String, CypherType] = physicalColumns.map(c => c -> df.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue] = df.toLocalIterator().asScala.map { row =>
      physicalColumns.map(c => c -> CypherValue(row.get(row.fieldIndex(c)))).toMap
    }

    override def size: Long = df.count()

    override def select(col: (String, String), cols: (String, String)*): DataFrameTable = {
      val columns = col +: cols
      if (df.columns.toSeq == columns.map { case (_, alias) => alias }) {
        df
      } else {
        // Spark interprets dots in column names as struct accessors. Hence, we need to escape column names by default.
        df.select(columns.map{ case (colName, alias) => df.col(s"`$colName`").as(alias) }: _*)
      }
    }

    override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {
      df.where(expr.asSparkSQLExpr(header, df, parameters))
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

    override def drop(cols: String*): DataFrameTable = {
      df.drop(cols: _*)
    }

    override def orderBy(sortItems: (Expr, Order)*)
      (implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {
      val mappedSortItems = sortItems.map { case (expr, order) =>
        val mappedExpr = expr.asSparkSQLExpr(header, df, parameters)
        order match {
          case Ascending => mappedExpr.asc
          case Descending => mappedExpr.desc
        }
      }
      df.orderBy(mappedSortItems: _*)
    }

    override def skip(items: Long): DataFrameTable = {
      // TODO: Replace with data frame based implementation ASAP
      df.sparkSession.createDataFrame(
        df.rdd
          .zipWithIndex()
          .filter(pair => pair._2 >= items)
          .map(_._1),
        df.toDF().schema
      )
    }

    override def limit(items: Long): DataFrameTable = {
      if (items > Int.MaxValue) throw IllegalArgumentException("an integer", items)
      df.limit(items.toInt)
    }

    override def group(by: Set[Var], aggregations: Set[(Aggregator, (String, CypherType))])
      (implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {

      def withInnerExpr(expr: Expr)(f: Column => Column) =
        f(expr.asSparkSQLExpr(header, df, parameters))

      val data: Either[RelationalGroupedDataset, DataFrame] =
        if (by.nonEmpty) {
          val columns = by.flatMap { expr =>
            val withChildren = header.ownedBy(expr)
            withChildren.map(e => withInnerExpr(e)(identity))
          }
          Left(df.groupBy(columns.toSeq: _*))
        } else {
          Right(df)
        }

      val sparkAggFunctions = aggregations.map {
        case (aggFunc, (columnName, cypherType)) =>
          aggFunc match {
            case Avg(expr) =>
              withInnerExpr(expr)(
                functions
                  .avg(_)
                  .cast(cypherType.getSparkType)
                  .as(columnName))

            case CountStar(_) =>
              functions.count(functions.lit(0)).as(columnName)

            // TODO: Consider not implicitly projecting the aggFunc expr here, but rewriting it into a variable in logical planning or IR construction
            case Count(expr, distinct) => withInnerExpr(expr) { column =>
              val count = {
                if (distinct) functions.countDistinct(column)
                else functions.count(column)
              }
              count.as(columnName)
            }

            case Max(expr) =>
              withInnerExpr(expr)(functions.max(_).as(columnName))

            case Min(expr) =>
              withInnerExpr(expr)(functions.min(_).as(columnName))

            case Sum(expr) =>
              withInnerExpr(expr)(functions.sum(_).as(columnName))

            case Collect(expr, distinct) => withInnerExpr(expr) { column =>
              val list = {
                if (distinct) functions.collect_set(column)
                else functions.collect_list(column)
              }
              // sort for deterministic aggregation results
              val sorted = functions.sort_array(list)
              sorted.as(columnName)
            }

            case x =>
              throw NotImplementedException(s"Aggregation function $x")
          }
      }

      data.fold(
        _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*),
        _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*)
      )
    }

    override def unionAll(other: DataFrameTable): DataFrameTable = {
      val leftTypes = df.schema.fields.flatMap(_.toCypherType)
      val rightTypes = other.df.schema.fields.flatMap(_.toCypherType)

      leftTypes.zip(rightTypes).foreach {
        case (leftType, rightType) if !leftType.nullable.couldBeSameTypeAs(rightType.nullable) =>
          throw IllegalArgumentException(
            "Equal column data types for union all (differing nullability is OK)",
            s"Left fields:  ${df.schema.fields.mkString(", ")}\n\tRight fields: ${other.df.schema.fields.mkString(", ")}")
        case _ =>
      }

      df.union(other.df)
    }

    override def join(other: DataFrameTable, joinType: JoinType, joinCols: (String, String)*): DataFrameTable = {
      val joinTypeString = joinType match {
        case InnerJoin => "inner"
        case LeftOuterJoin => "left_outer"
        case RightOuterJoin => "right_outer"
        case FullOuterJoin => "full_outer"
        case CrossJoin => "cross"
      }

      joinType match {
        case CrossJoin =>
          df.crossJoin(other.df)

        case _ =>

          // TODO: the join produced corrupt data when the previous operator was a cross. We work around that by using a
          // subsequent select. This can be removed, once https://issues.apache.org/jira/browse/SPARK-23855 is solved or we
          // upgrade to Spark 2.3.0
          val potentiallyCorruptedResult = df.safeJoin(other.df, joinCols, joinTypeString)
          potentiallyCorruptedResult.select("*")
      }
    }

    override def distinct: DataFrameTable = distinct(df.columns: _*)

    // workaround for https://issues.apache.org/jira/browse/SPARK-26572
    override def distinct(colNames: String*): DataFrameTable = {
      val uniqueSuffix = "_temp_distinct"

      val originalColNames = df.columns

      val renamings = originalColNames.map { c =>
        if (colNames.contains(c)) c -> s"$c$uniqueSuffix"
        else c -> c
      }.toMap

      val renamedDf = df.safeRenameColumns(colNames.map(c => c -> renamings(c)).toMap)

      val extractRowFromGrouping = originalColNames.map(c => functions.first(renamings(c)) as c)
      val groupedDf = renamedDf
        .groupBy(colNames.map(c => functions.col(renamings(c))): _*)
        .agg(extractRowFromGrouping.head, extractRowFromGrouping.tail: _*)

      groupedDf.safeDropColumns(colNames.map(renamings): _*)
    }

    override def withColumnsRenamed(columnRenamings: Map[String, String]): DataFrameTable = {
      df.safeRenameColumns(columnRenamings)
    }

    override def cache(): DataFrameTable = {
      val planToCache = df.queryExecution.analyzed
      if (df.sparkSession.sharedState.cacheManager.lookupCachedData(planToCache).nonEmpty) {
        df.sparkSession.sharedState.cacheManager.cacheQuery(df, None, StorageLevel.MEMORY_ONLY)
      }
      this
    }

    override def show(rows: Int): Unit = df.show(rows)

    def persist(): DataFrameTable = df.persist()

    def persist(newLevel: StorageLevel): DataFrameTable = df.persist(newLevel)

    def unpersist(): DataFrameTable = df.unpersist()

    def unpersist(blocking: Boolean): DataFrameTable = df.unpersist(blocking)
  }

  implicit class DataFrameOps(val df: DataFrame) extends AnyVal {

    def safeJoin(other: DataFrame, joinCols: Seq[(String, String)], joinType: String): DataFrame = {
      require(joinCols.map(_._1).forall(col => !other.columns.contains(col)))
      require(joinCols.map(_._2).forall(col => !df.columns.contains(col)))

      val joinExpr = joinCols.map {
        case (l, r) => df.col(l) === other.col(r)
      }.reduce((acc, expr) => acc && expr)

      df.join(other, joinExpr, joinType)
    }

    def safeDropColumns(names: String*): DataFrame = {
      val nonExistentColumns = names.toSet -- df.columns
      require(nonExistentColumns.isEmpty,
        s"Cannot drop column(s) ${nonExistentColumns.map(c => s"`$c`").mkString(", ")}. They do not exist.")
      df.drop(names: _*)
    }

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
