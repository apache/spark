package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.dialect.Dialect

/**
 * Created by Sally on 2015/3/8.
 */
object HiveQlDialect extends Dialect {
  val name = "hiveql"

  override def description = "Hive query language dialect"

  override def parse(sql: String): LogicalPlan = {
    HiveQl.parseSql(sql)
  }
}
