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
sealed trait CatalystIdentifier {
  private var _catalog: Option[String] = None
  val identifier: String

  def database: Option[String]
  def withCatalog(catalog: String): Unit = this._catalog = Option(catalog)
  def catalog: Option[String] = _catalog

  /*
   * Escapes back-ticks within the identifier name with double-back-ticks.
   */
  private def quoteIdentifier(name: String): String = name.replace("`", "``")

  def quotedString: String = {
    val replacedId = quoteIdentifier(identifier)
    val replacedDb = database.map(quoteIdentifier(_))

    if (catalog.isDefined && replacedDb.isDefined) {
      s"`${catalog.get}`.`${replacedDb.get}`.`$replacedId`"
    } else if (replacedDb.isDefined) {
       s"`${replacedDb.get}`.`$replacedId`"
    } else {
      s"`$replacedId`"
    }
  }

  def unquotedString: String = {
    if (catalog.isDefined && database.isDefined) {
      s"${catalog.get}.${database.get}.$identifier"
    } else if (database.isDefined) {
      s"${database.get}.$identifier"
    } else {
      identifier
    }
  }

  override def toString: String = quotedString
}

/**
 * Encapsulates an identifier that is either a alias name or an identifier that has table
 * name and a qualifier.
 * The SubqueryAlias node keeps track of the qualifier using the information in this structure
 * @param name - Is an alias name or a table name
 * @param qualifier - Is a qualifier
 */
case class AliasIdentifier(name: String, qualifier: Seq[String]) {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def this(identifier: String) = this(identifier, Seq())

  override def toString: String = (qualifier :+ name).quoted
}

object AliasIdentifier {
  def apply(name: String): AliasIdentifier = new AliasIdentifier(name)
}

/**
 * Identifies a table in a database.
 * If `database` is not defined, the current database is used.
 * When we register a permanent function in the FunctionRegistry, we use
 * unquotedString as the function name.
 */
case class TableIdentifier(table: String, database: Option[String])
  extends CatalystIdentifier {

  override val identifier: String = table

  def this(table: String) = this(table, None)

  def copy(
      table: String = this.table,
      database: Option[String] = this.database): TableIdentifier = {
    val ident = TableIdentifier(table, database)
    this.catalog.foreach(ident.withCatalog)
    ident
  }
}

/** A fully qualified identifier for a table (i.e., database.tableName) */
case class QualifiedTableName(database: String, name: String) {
  override def toString: String = s"$database.$name"
}

object TableIdentifier {
  def apply(tableName: String): TableIdentifier = new TableIdentifier(tableName)
}


/**
 * Identifies a function in a database.
 * If `database` is not defined, the current database is used.
 */
case class FunctionIdentifier(funcName: String, database: Option[String])
  extends CatalystIdentifier {

  override val identifier: String = funcName

  def this(funcName: String) = this(funcName, None)

  override def toString: String = unquotedString

  def copy(
      funcName: String = this.funcName,
      database: Option[String] = this.database): FunctionIdentifier = {
    val ident = FunctionIdentifier(funcName, database)
    this.catalog.foreach(ident.withCatalog)
    ident
  }
}

object FunctionIdentifier {
  def apply(funcName: String): FunctionIdentifier = new FunctionIdentifier(funcName)
}
