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
 */

package org.apache.spark.sql.connect.pipelines

// scalastyle:off
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.pipelines.utils.TestGraphRegistrationContext.{DEFAULT_CATALOG, DEFAULT_DATABASE}
import org.scalatest.funsuite.AnyFunSuite

case class TestPipelineSpec(catalog: String, database: String, include: Seq[String])

case class File(name: String, contents: String)

trait PipelineReference {}

trait UpdateReference {}

trait APITest
  extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers {

  protected def spark: SparkSession
  protected def catalogInPipelineSpec: Option[String]
  protected def databaseInPipelineSpec: Option[String]

  def createAndRunPipeline(
      spec: TestPipelineSpec,
      sources: Seq[File]): (PipelineReference, UpdateReference)
  def awaitPipelineUpdateTermination(update: UpdateReference): Unit
  def stopPipelineUpdate(update: UpdateReference): Unit

  // Example test
  test("Pipeline dataset can be referenced in CTE") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("*/definition.sql"))

    val sources = Seq(
      File(
        name = "/definition.sql",
        contents = """
                     |CREATE MATERIALIZED VIEW a AS SELECT 1;
                     |CREATE MATERIALIZED VIEW d AS
                     |WITH c AS (
                     | WITH b AS (
                     |   SELECT * FROM a
                     | )
                     | SELECT * FROM b
                     |)
                     |SELECT * FROM c;
                     |""".stripMargin
      )
    )

    val (_, update) = createAndRunPipeline(pipelineSpec, sources)
    awaitPipelineUpdateTermination(update)

    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.d"),
      Seq(Row(1))
    )
  }

  test("Pipeline dataset can be referenced in subquery") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("*/definition.sql"))

    val sources = Seq(
      File(
        name = "/definition.sql",
        contents = """
                     |CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
                     |CREATE MATERIALIZED VIEW b AS SELECT * FROM RANGE(5)
                     |WHERE id = (SELECT max(id) FROM a);
                     |""".stripMargin
      )
    )

    val (_, update) = createAndRunPipeline(pipelineSpec, sources)
    awaitPipelineUpdateTermination(update)

    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.b"),
      Seq(Row(4))
    )
  }

}

