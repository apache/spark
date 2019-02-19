package org.apache.spark.graph.cypher

import org.apache.spark.graph.cypher.SparkTable.DataFrameTable
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory

object SparkCypherSession {
  def create(implicit sparkSession: SparkSession): SparkCypherSession = new SparkCypherSession(sparkSession)
}

class SparkCypherSession(val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] {

  override type Result = RelationalCypherResult[DataFrameTable]
  override type Records = SparkCypherRecords
  override type Graph = RelationalCypherGraph[DataFrameTable]

  implicit def sparkCypherSession: SparkCypherSession = this

  override val records: SparkCypherRecordsFactory = SparkCypherRecordsFactory()

  override val graphs: RelationalCypherGraphFactory[DataFrameTable] = new RelationalCypherGraphFactory[DataFrameTable]() {
    override implicit val session: RelationalCypherSession[DataFrameTable] = sparkCypherSession
  }

  override def entityTables: RelationalEntityTableFactory[DataFrameTable] = ???
}


