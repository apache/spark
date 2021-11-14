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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical.{RebalancePartitions, RepartitionByExpression, Sort}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, InsertIntoDataSourceDirCommand}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.internal.SQLConf.REPARTITION_WRITING_DATASOURCE
import org.apache.spark.sql.test.SharedSparkSession

class RepartitionWritingDataSourceSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val defaultRepartitionWritingDataSource = conf.repartitionWritingDataSource

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.setConf(REPARTITION_WRITING_DATASOURCE, true)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    conf.setConf(REPARTITION_WRITING_DATASOURCE, defaultRepartitionWritingDataSource)
  }

  test("Repartition dynamic column for CreateDataSourceTableAsSelectCommand") {
    withTable("t1") {
      sql(
        """
          |CREATE TABLE t1 USING parquet
          |CLUSTERED BY (key) SORTED BY(value) into 10 buckets
          |AS (SELECT id AS key, id AS value FROM range(5))
          |""".stripMargin).queryExecution.analyzed match {
        case CreateDataSourceTableAsSelectCommand(_, _,
            Sort(_, false, RepartitionByExpression(_, _, Some(10))), _) =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }
    }

    withTable("t1") {
      sql(
        """
          |CREATE TABLE t1 USING parquet
          |CLUSTERED BY (key) into 10 buckets
          |AS (SELECT id AS key, id AS value FROM range(5))
          |""".stripMargin).queryExecution.analyzed match {
        case CreateDataSourceTableAsSelectCommand(_, _,
        RepartitionByExpression(_, _, Some(10)), _) =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }
    }

    withTable("t1") {
      sql(
        """
          |CREATE TABLE t1 USING parquet
          |AS (SELECT id AS key, id AS value FROM range(5))
          |""".stripMargin).queryExecution.analyzed match {
        case CreateDataSourceTableAsSelectCommand(_, _, RebalancePartitions(Nil, _), _) =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }
    }

    withTable("t1") {
      sql(
        """
          |CREATE TABLE t1 USING parquet
          |PARTITIONED BY (key)
          |AS (SELECT id AS key, id AS value FROM range(5))
          |""".stripMargin).queryExecution.analyzed match {
        case CreateDataSourceTableAsSelectCommand(_, _, RepartitionByExpression(partExps, _, _), _)
          if partExps.flatMap(_.references.map(_.name)) == Seq("key") =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }
    }

    withTable("t1") {
      sql(
        """
          |CREATE TABLE t1 USING parquet
          |PARTITIONED BY (part) CLUSTERED BY (key) SORTED BY(value) into 10 buckets
          |AS (SELECT id % 2 AS part, id as key, id AS value FROM range(5))
          |""".stripMargin).queryExecution.analyzed match {
        case CreateDataSourceTableAsSelectCommand(_, _,
            Sort(_, false, RepartitionByExpression(partExps, _, Some(10))), _)
          if partExps.flatMap(_.references.map(_.name)) == Seq("part", "key") =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }
    }
  }

  test("Repartition dynamic column for InsertIntoHadoopFsRelationCommand") {
    withTable("t1") {
      sql(
        s"""CREATE TABLE t1 (
           |  id BIGINT,
           |  p1 BIGINT,
           |  p2 BIGINT)
           |USING PARQUET
           |PARTITIONED BY (p1, p2)""".stripMargin)

      sql(
        """
          |INSERT INTO t1
          |SELECT id, id AS  p1, id AS p2 FROM range(5)
          |""".stripMargin).queryExecution.analyzed match {
        case InsertIntoHadoopFsRelationCommand(
            _, _, _, _, _, _, _, RepartitionByExpression(partExps, _, _), _, _, _, _)
          if partExps.flatMap(_.references.map(_.name)) == Seq("p1", "p2") =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }

      sql(
        """
          |INSERT INTO TABLE t1
          |PARTITION(p1 = 1, p2)
          |SELECT id, id AS p2 FROM range(5)
          |""".stripMargin).queryExecution.analyzed match {
        case InsertIntoHadoopFsRelationCommand(
            _, _, _, _, _, _, _, RepartitionByExpression(partExps, _, _), _, _, _, _)
          if partExps.flatMap(_.references.map(_.name)) == Seq("p2") =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }

      sql(
        """
          |INSERT INTO TABLE t1
          |PARTITION(p1 = 1, P2)
          |SELECT id, id % 2 AS P2 FROM range(5)
          |""".stripMargin).queryExecution.analyzed match {
        case InsertIntoHadoopFsRelationCommand(
            _, _, _, _, _, _, _, RepartitionByExpression(partExps, _, _), _, _, _, _)
          if partExps.flatMap(_.references.map(_.name)) == Seq("p2") =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }

      sql(
        """
          |INSERT INTO t1 PARTITION(p1 = 1, p2 = 2)
          |SELECT id FROM range(5)
          |""".stripMargin).queryExecution.analyzed match {
        case InsertIntoHadoopFsRelationCommand(
            _, _, _, _, _, _, _, RebalancePartitions(Nil, _), _, _, _, _) =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }
    }
  }

  test("Repartition for InsertIntoDataSourceCommand") {
    withTempPath { tempPath =>
      sql(
        s"""
           | INSERT OVERWRITE DIRECTORY '${tempPath.toURI.getPath}'
           | USING parquet
           | SELECT 1 AS a, 'c' AS b
         """.stripMargin).queryExecution.analyzed match {
        case InsertIntoDataSourceDirCommand(_, _, RebalancePartitions(Nil, _), _) =>
        case other =>
          fail(
            s"""
               |== FAIL: Plans do not match ===
               |${rewriteNameFromAttrNullability(other).treeString}
            """.stripMargin)
      }
    }
  }
}
