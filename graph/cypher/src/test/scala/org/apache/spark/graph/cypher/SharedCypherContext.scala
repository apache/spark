package org.apache.spark.graph.cypher

import org.apache.spark.sql.test.SharedSQLContext

trait SharedCypherContext extends SharedSQLContext {

  private var _cypherEngine: SparkCypherSession = _

  protected implicit def cypherEngine: SparkCypherSession = _cypherEngine

  override def beforeAll() {
    super.beforeAll()
    _cypherEngine = SparkCypherSession.create
  }

  protected override def afterAll(): Unit = {
    _cypherEngine = null
    super.afterAll()
  }
}
