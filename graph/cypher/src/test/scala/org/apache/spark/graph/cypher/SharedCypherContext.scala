package org.apache.spark.graph.cypher

import org.apache.spark.graph.api.CypherSession
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.Suite

trait SharedCypherContext extends SharedSparkSession { self: Suite =>

  private var _cypherEngine: SparkCypherSession = _

  protected implicit def cypherSession: CypherSession = _cypherEngine

  def internalCypherSession: SparkCypherSession = _cypherEngine

  override def beforeAll() {
    super.beforeAll()
    _cypherEngine = SparkCypherSession.createInternal
  }

  protected override def afterAll(): Unit = {
    _cypherEngine = null
    super.afterAll()
  }
}
