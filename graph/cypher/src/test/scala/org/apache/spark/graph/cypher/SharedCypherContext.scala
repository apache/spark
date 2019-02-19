package org.apache.spark.graph.cypher

import org.apache.spark.sql.test.SharedSQLContext

trait SharedCypherContext extends SharedSQLContext {

  private var _sparkCypher: SparkCypherSession = _

  protected implicit def sparkCypher: SparkCypherSession = _sparkCypher

  override def beforeAll() {
    super.beforeAll()
    _sparkCypher = SparkCypherSession.create
  }

  protected override def afterAll(): Unit = {
    _sparkCypher = null
    super.afterAll()
  }
}
