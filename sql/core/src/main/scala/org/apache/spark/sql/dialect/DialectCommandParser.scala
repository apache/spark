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

package org.apache.spark.sql.dialect

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{DropDialectCommand, SwitchDialectCommand, ShowDialectsCommand, AddDialectCommand}

private[sql] object DialectCommandParser extends AbstractSparkSQLParser with Logging {

  def apply(input: String, exceptionOnError: Boolean): Option[LogicalPlan] = {
    try {
      Some(apply(input))
    } catch {
      case _ if !exceptionOnError => None
      case x: Throwable => throw x
    }
  }

  protected val CREATE   = Keyword("CREATE")
  protected val DIALECT  = Keyword("DIALECT")
  protected val DIALECTS = Keyword("DIALECTS")
  protected val DROP     = Keyword("DROP")
  protected val EXTENDED = Keyword("EXTENDED")
  protected val SHOW     = Keyword("SHOW")
  protected val USE      = Keyword("USE")
  protected val USING    = Keyword("USING")

  protected def start: Parser[LogicalPlan] =
    addDialect | showDialects | showCurrentDialect | switchDialect | dropDialect

  protected lazy val className: Parser[String] = rep1sep(ident, ".") ^^ { case s => s.mkString(".")}

  protected lazy val addDialect =
    (CREATE ~ DIALECT) ~> ident ~ (USING ~> className) ^^ {
      case n ~ c => AddDialectCommand(n, c)
    }

  protected lazy val showDialects =
    SHOW ~> EXTENDED.? <~ DIALECTS ^^ {
      case e => ShowDialectsCommand(e.isDefined, false)
    }

  protected lazy val showCurrentDialect =
    SHOW ~> EXTENDED.? <~ DIALECT ^^ {
      case e => ShowDialectsCommand(e.isDefined, true)
    }

  protected lazy val switchDialect =
    USE ~ DIALECT ~> ident ^^ {
      case i => SwitchDialectCommand(i)
    }

  protected lazy val dropDialect =
    DROP ~ DIALECT ~> ident ^^ {
      case i => DropDialectCommand(i)
    }
}

