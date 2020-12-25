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

package org.apache.spark.sql.sources

import java.net.URI

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder, StructType}

class TestOptionsSource extends SchemaRelationProvider with CreatableRelationProvider {

  // This is used in the read path.
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    new TestOptionsRelation(parameters)(sqlContext.sparkSession)
  }

  // This is used in the write path.
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    new TestOptionsRelation(parameters)(sqlContext.sparkSession)
  }
}

class TestOptionsRelation(val options: Map[String, String])(@transient val session: SparkSession)
  extends BaseRelation {

  override def sqlContext: SQLContext = session.sqlContext

  def pathOption: Option[String] = options.get("path")

  // We can't get the relation directly for write path, here we put the path option in schema
  // metadata, so that we can test it later.
  override def schema: StructType = {
    val metadataWithPath = pathOption.map { path =>
      new MetadataBuilder().putString("path", path).build()
    }
    new StructType().add("i", IntegerType, true, metadataWithPath.getOrElse(Metadata.empty))
  }
}

class PathOptionSuite extends DataSourceTest with SharedSparkSession {

  test("path option always exist") {
    withTable("src") {
      sql(
        s"""
           |CREATE TABLE src(i int)
           |USING ${classOf[TestOptionsSource].getCanonicalName}
           |OPTIONS (PATH '/tmp/path')
        """.stripMargin)
      assert(getPathOption("src").map(makeQualifiedPath) == Some(makeQualifiedPath("/tmp/path")))
    }

    // should exist even path option is not specified when creating table
    withTable("src") {
      sql(s"CREATE TABLE src(i int) USING ${classOf[TestOptionsSource].getCanonicalName}")
      assert(getPathOption("src").map(makeQualifiedPath) == Some(defaultTablePath("src")))
    }
  }

  test("path option also exist for write path") {
    withTable("src") {
      withTempPath { p =>
        sql(
          s"""
            |CREATE TABLE src
            |USING ${classOf[TestOptionsSource].getCanonicalName}
            |OPTIONS (PATH '${p.toURI}')
            |AS SELECT 1
          """.stripMargin)
        assert(
          spark.table("src").schema.head.metadata.getString("path") ==
          p.toURI.toString)
      }
    }

    // should exist even path option is not specified when creating table
    withTable("src") {
      sql(
        s"""
           |CREATE TABLE src
           |USING ${classOf[TestOptionsSource].getCanonicalName}
           |AS SELECT 1
          """.stripMargin)
      assert(
        makeQualifiedPath(spark.table("src").schema.head.metadata.getString("path")) ==
        defaultTablePath("src"))
    }
  }

  test("path option always represent the value of table location") {
    withTable("src") {
      sql(
        s"""
           |CREATE TABLE src(i int)
           |USING ${classOf[TestOptionsSource].getCanonicalName}
           |OPTIONS (PATH '/tmp/path')""".stripMargin)
      sql("ALTER TABLE src SET LOCATION '/tmp/path2'")
      assert(getPathOption("src") ==
        Some(CatalogUtils.URIToString(makeQualifiedPath("/tmp/path2"))))
    }

    withTable("src", "src2") {
      sql(s"CREATE TABLE src(i int) USING ${classOf[TestOptionsSource].getCanonicalName}")
      sql("ALTER TABLE src RENAME TO src2")
      assert(getPathOption("src2").map(makeQualifiedPath) == Some(defaultTablePath("src2")))
    }
  }

  private def getPathOption(tableName: String): Option[String] = {
    spark.table(tableName).queryExecution.analyzed.collect {
      case LogicalRelation(r: TestOptionsRelation, _, _, _) => r.pathOption
    }.head
  }

  private def defaultTablePath(tableName: String): URI = {
    spark.sessionState.catalog.defaultTablePath(TableIdentifier(tableName))
  }
}
