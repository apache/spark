/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.cypher

import org.apache.spark.SparkConf
import org.apache.spark.graph.api.CypherSession
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.Suite

trait SharedCypherContext extends SharedSparkSession {
  self: Suite =>

  private var _cypherEngine: SparkCypherSession = _

  protected implicit def cypherSession: CypherSession = _cypherEngine

  def internalCypherSession: SparkCypherSession = _cypherEngine

  override protected def sparkConf: SparkConf = super.sparkConf
    // Required for left outer join without join expressions in OPTIONAL MATCH (leads to cartesian product)
    .set("spark.sql.crossJoin.enabled", "true")

  override def beforeAll() {
    super.beforeAll()
    _cypherEngine = SparkCypherSession.createInternal
  }

  protected override def afterAll(): Unit = {
    _cypherEngine = null
    super.afterAll()
  }
}
