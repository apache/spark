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

package org.apache.spark.sql.hive

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.hive.execution.{AddJar, AddFile, HiveNativeCommand}

/**
 * A parser that recognizes all HiveQL constructs together with Spark SQL specific extensions.
 */
private[hive] class ExtendedHiveQlParser extends AbstractSparkSQLParser {
  // Keyword is a convention with AbstractSparkSQLParser, which will scan all of the `Keyword`
  // properties via reflection the class in runtime for constructing the SqlLexical object
  protected val ADD = Keyword("ADD")
  protected val DFS = Keyword("DFS")
  protected val FILE = Keyword("FILE")
  protected val JAR = Keyword("JAR")

  protected lazy val start: Parser[LogicalPlan] = dfs | addJar | addFile | hiveQl

  protected lazy val hiveQl: Parser[LogicalPlan] =
    restInput ^^ {
      case statement => HiveQl.createPlan(statement.trim)
    }

  protected lazy val dfs: Parser[LogicalPlan] =
    DFS ~> wholeInput ^^ {
      case command => HiveNativeCommand(command.trim)
    }

  private lazy val addFile: Parser[LogicalPlan] =
    ADD ~ FILE ~> restInput ^^ {
      case input => AddFile(input.trim)
    }

  private lazy val addJar: Parser[LogicalPlan] =
    ADD ~ JAR ~> restInput ^^ {
      case input => AddJar(input.trim)
    }
}
