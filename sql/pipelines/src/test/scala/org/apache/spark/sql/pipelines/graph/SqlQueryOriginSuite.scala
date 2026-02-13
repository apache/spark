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
package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.pipelines.Language
import org.apache.spark.sql.pipelines.utils.PipelineTest
import org.apache.spark.sql.test.SharedSparkSession

class SqlQueryOriginSuite extends PipelineTest with SharedSparkSession {
  test("basic test") {
    val sqlQueryOrigins = SqlGraphRegistrationContext.splitSqlFileIntoQueries(
      spark,
      sqlFilePath = "file.sql",
      sqlFileText =
        """-- comment 1
          |CREATE MATERIALIZED VIEW a.b.c AS SELECT 1;
          |
          |USE DATABASE d ; -- comment 2
          |""".stripMargin
    ).map(_.queryOrigin)
    assert(sqlQueryOrigins == Seq(
      QueryOrigin(
        sqlText = Option(
          """CREATE MATERIALIZED VIEW a.b.c AS SELECT 1""".stripMargin),
        filePath = Option("file.sql"),
        language = Option(Language.Sql()),
        startPosition = Option(13),
        line = Option(2)
      ),
      QueryOrigin(
        sqlText = Option(
          """USE DATABASE d """.stripMargin),
        filePath = Option("file.sql"),
        language = Option(Language.Sql()),
        startPosition = Option(58),
        line = Option(4)
      )
    ))
  }

  test("\\n in sql file is not considered a new line") {
    val sqlQueryOrigins = SqlGraphRegistrationContext.splitSqlFileIntoQueries(
      spark,
      sqlFilePath = "file.sql",
      sqlFileText =
        """CREATE STREAMING TABLE `a.\n` AS SELECT "\n";
          |CREATE VIEW my_view AS SELECT * FROM `a.\n`;
          |""".stripMargin
    ).map(_.queryOrigin)
    assert(sqlQueryOrigins == Seq(
      QueryOrigin(
        sqlText = Option("CREATE STREAMING TABLE `a.\\n` AS SELECT \"\\n\""),
        filePath = Option("file.sql"),
        language = Option(Language.Sql()),
        startPosition = Option(0),
        line = Option(1)
      ),
      QueryOrigin(
        sqlText = Option("CREATE VIEW my_view AS SELECT * FROM `a.\\n`"),
        filePath = Option("file.sql"),
        language = Option(Language.Sql()),
        startPosition = Option(46),
        line = Option(2)
      )
    ))
  }

  test("White space is accounted for in startPosition") {
    val sqlQueryOrigins = SqlGraphRegistrationContext.splitSqlFileIntoQueries(
      spark,
      sqlFilePath = "file.sql",
      sqlFileText =
        s"""
           |     ${"\t"}CREATE FLOW f AS INSERT INTO t BY NAME SELECT 1;
           |""".stripMargin
    ).map(_.queryOrigin)
    assert(sqlQueryOrigins == Seq(
      QueryOrigin(
        sqlText = Option("CREATE FLOW f AS INSERT INTO t BY NAME SELECT 1"),
        filePath = Option("file.sql"),
        language = Option(Language.Sql()),
        // 1 new line, 5 spaces, 1 tab
        startPosition = Option(7),
        line = Option(2)
      )
    ))
  }

  test("Multiline SQL statement line number is the first line of the statement") {
    val sqlQueryOrigins = SqlGraphRegistrationContext.splitSqlFileIntoQueries(
      spark,
      sqlFilePath = "file.sql",
      sqlFileText =
        s"""
           |CREATE
           |MATERIALIZED VIEW mv
           |AS
           |SELECT 1;
           |""".stripMargin
    ).map(_.queryOrigin)
    assert(sqlQueryOrigins == Seq(
      QueryOrigin(
        sqlText = Option(
          s"""CREATE
             |MATERIALIZED VIEW mv
             |AS
             |SELECT 1""".stripMargin
        ),
        filePath = Option("file.sql"),
        language = Option(Language.Sql()),
        // 1 new line
        startPosition = Option(1),
        line = Option(2)
      )
    ))
  }

  test("Preceeding comment is ommitted from sql text") {
    val sqlQueryOrigins = SqlGraphRegistrationContext.splitSqlFileIntoQueries(
      spark,
      sqlFilePath = "file.sql",
      sqlFileText =
        s"""
           |-- comment
           |CREATE MATERIALIZED VIEW mv -- another comment
           |AS SELECT 1;
           |""".stripMargin
    ).map(_.queryOrigin)
    assert(sqlQueryOrigins == Seq(
      QueryOrigin(
        sqlText = Option(
          s"""CREATE MATERIALIZED VIEW mv -- another comment
             |AS SELECT 1""".stripMargin
        ),
        filePath = Option("file.sql"),
        language = Option(Language.Sql()),
        // 1 new line, 10 chars for preceding comment, another new line
        startPosition = Option(12),
        line = Option(3)
      )
    ))
  }

  test("Semicolon in string literal does not cause statement split") {
    val sqlQueryOrigins = SqlGraphRegistrationContext.splitSqlFileIntoQueries(
      spark,
      sqlFilePath = "file.sql",
      sqlFileText = "CREATE TEMPORARY VIEW v AS SELECT 'my ; string';"
    ).map(_.queryOrigin)
    assert(sqlQueryOrigins == Seq(
      QueryOrigin(
        sqlText = Option("CREATE TEMPORARY VIEW v AS SELECT 'my ; string'"),
        filePath = Option("file.sql"),
        language = Option(Language.Sql()),
        startPosition = Option(0),
        line = Option(1)
      )
    ))
  }
}
