package org.apache.spark.sql.execution.stat

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


private[sql] object ContingencyTable {

  /** Generate a table of frequencies for the elements of two columns. */
  private[sql] def crossTabulate(df: DataFrame, col1: String, col2: String): DataFrame = {
    val tableName = s"${col1}_$col2"
    val distinctVals = df.select(countDistinct(col1), countDistinct(col2)).collect().head
    val distinctCol1 = distinctVals.getLong(0)
    val distinctCol2 = distinctVals.getLong(1)

    require(distinctCol1 < Int.MaxValue, s"The number of distinct values for $col1, can't " +
      s"exceed Int.MaxValue. Currently $distinctCol1")
    require(distinctCol2 < Int.MaxValue, s"The number of distinct values for $col2, can't " +
      s"exceed Int.MaxValue. Currently $distinctCol2")
    // Aggregate the counts for the two columns
    val allCounts =
      df.groupBy(col1, col2).agg(col(col1), col(col2), count("*")).orderBy(col1, col2).collect()
    // Pivot the table
    val pivotedTable = allCounts.grouped(distinctCol2.toInt).toArray
    // Get the column names (distinct values of col2)
    val headerNames = pivotedTable.head.map(r => StructField(r.get(1).toString, LongType))
    val schema = StructType(StructField(tableName, StringType) +: headerNames)
    val table = pivotedTable.map { rows =>
      // the value of col1 is the first value, the rest are the counts
      val rowValues = rows.head.get(0).toString +: rows.map(_.getLong(2))
      Row(rowValues:_*)
    }
    new DataFrame(df.sqlContext, LocalRelation(schema.toAttributes, table))
  }

}
