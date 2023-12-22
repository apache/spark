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

package org.apache.spark.sql.crossdbms

import java.io.File

// scalastyle:off line.size.limit
/**
 * IF YOU ADDED A NEW SQL TEST AND THIS SUITE IS FAILING, READ THIS:
 * Your new SQL test is automatically opted into this suite. It is likely failing because it is not
 * compatible with the default DBMS (currently postgres). You have two options:
 * 1. (Recommended) Modify your queries to be compatible with both systems, and generate golden
 *    files with the instructions below. This is recommended because it will run your queries
 *    against postgres, providing higher correctness testing confidence, and you won't have to
 *    manually verify the golden files generated with your test.
 * 2. Add this line to your .sql file: --ONLY_IF spark
 *
 * To re-generate golden files for entire suite, either run:
 * 1. (Recommended) You need Docker on your machine. Install Docker and run the following command:
 * {{{
 *   bash ./bin/generate_golden_files_with_postgres.sh
 * }}}
 * 2.
 *   a. You need to have a Postgres server up before running this test.
 *      i. Install PostgreSQL. On a mac: `brew install postgresql@13`
 *      ii. After installing PostgreSQL, start the database server, then create a role named pg with
 *      superuser permissions: `createuser -s postgres` OR `psql> CREATE role postgres superuser`
 *   b. Run the following command:
 *     {{{
 *       SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.PostgreSQLQueryTestSuite"
 *     }}}
 *
 * To indicate that the SQL file is not eligible for testing with this suite, add the following
 * comment into the input file:
 * {{{
 *   --ONLY_IF spark
 * }}}
 *
 * And then, to run the entire test suite, with the default cross DBMS:
 * {{{
 *   build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.PostgreSQLQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, e.g. `describe.sql`, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.PostgreSQLQueryTestSuite -- -z describe.sql"
 * }}}
 */
class PostgreSQLQueryTestSuite extends CrossDbmsQueryTestSuite {

  protected def crossDbmsToGenerateGoldenFiles: String = CrossDbmsQueryTestSuite.POSTGRES

  // Reduce scope to subquery tests for now. That is where most correctness issues are.
  override protected def customInputFilePath: String = new File(inputFilePath, "subquery").getAbsolutePath

  override protected def getConnection: Option[String] => JdbcSQLQueryTestRunner =
    (connection_url: Option[String]) => JdbcSQLQueryTestRunner(PostgresConnection(connection_url))

  override protected def preprocessingCommands = Seq(
    // Custom function `double` to imitate Spark's function, so that more tests are covered.
    """
      |CREATE OR REPLACE FUNCTION double(numeric_value numeric) RETURNS double precision
      |    AS 'select CAST($1 AS double precision);'
      |    LANGUAGE SQL
      |    IMMUTABLE
      |    RETURNS NULL ON NULL INPUT;
      |""".stripMargin
  )
}
