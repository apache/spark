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

package org.apache.spark.sql.catalyst

/**
 * An identifier that optionally specifies a database.
 *
 * Format (unquoted): "name" or "db.name"
 * Format (quoted): "`name`" or "`db`.`name`"
 */
sealed trait IdentifierWithDatabase {
  val identifier: String

  def database: Option[String]

  /*
   * Escapes back-ticks within the identifier name with double-back-ticks.
   */
  private def quoteIdentifier(name: String): String = name.replace("`", "``")

  def quotedString: String = {
    val replacedId = quoteIdentifier(identifier)
    val replacedDb = database.map(quoteIdentifier(_))

    if (replacedDb.isDefined) s"`${replacedDb.get}`.`$replacedId`" else s"`$replacedId`"
  }

  def unquotedString: String = {
    val replacedId = quoteIdentifier(identifier)
    val replacedDb = database.map(quoteIdentifier(_))

    if (replacedDb.isDefined) s"${replacedDb.get}.$replacedId" else replacedId
  }

  override def toString: String = quotedString
}


/**
 * Identifies a table in a database.
 * If `database` is not defined, the current database is used.
 * When we register a permanent function in the FunctionRegistry, we use
 * unquotedString as the function name.
 */
case class TableIdentifier(table: String, database: Option[String])
  extends IdentifierWithDatabase {

  override val identifier: String = table

  def this(table: String) = this(table, None)

}

object TableIdentifier {
  def apply(tableName: String): TableIdentifier = new TableIdentifier(tableName)
}


/**
 * Identifies a function in a database.
 * If `database` is not defined, the current database is used.
 */
case class FunctionIdentifier(funcName: String, database: Option[String])
  extends IdentifierWithDatabase {

  override val identifier: String = funcName

  def this(funcName: String) = this(funcName, None)

  override def toString: String = unquotedString
}

object FunctionIdentifier {
  def apply(funcName: String): FunctionIdentifier = new FunctionIdentifier(funcName)
}
