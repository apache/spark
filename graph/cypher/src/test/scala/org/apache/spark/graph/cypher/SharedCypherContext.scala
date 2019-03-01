package org.apache.spark.graph.cypher

import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.Suite

trait SharedCypherContext extends SharedSparkSession { self: Suite =>

  private var _cypherEngine: SparkCypherSession = _

  protected implicit def cypherSession: SparkCypherSession = _cypherEngine

  override def beforeAll() {
    super.beforeAll()
    _cypherEngine = SparkCypherSession.create
  }

  protected override def afterAll(): Unit = {
    _cypherEngine = null
    super.afterAll()
  }
}
