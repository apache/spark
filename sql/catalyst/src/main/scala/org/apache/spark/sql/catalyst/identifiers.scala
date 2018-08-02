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
 * An identifier that optionally specifies a database and catalog.
 *
 * Format (unquoted): "name" or "db.name"
 * Format (quoted): "`name`" or "`db`.`name`"
 */
sealed trait IdentifierWithOptionalDatabaseAndCatalog {
  val identifier: String

  def database: Option[String]

  def catalog: Option[String]

  /*
   * Escapes back-ticks within the identifier name with double-back-ticks.
   */
  private def quoteIdentifier(name: String): String = name.replace("`", "``")

  def quotedString: String = {
    // database is required if catalog is present
    assert(database.isDefined || catalog.isEmpty)
    def q(s: String): String = s"`${quoteIdentifier(s)}`"
    Seq(catalog.map(q), database.map(q), Some(q(identifier))).flatten.mkString(".")
  }

  def unquotedString: String = {
    Seq(catalog, database, Some(identifier)).flatten.mkString(".")
  }

  override def toString: String = quotedString
}

/**
 * Encapsulates an identifier that is either a alias name or an identifier that has table
 * name and optionally a database name.
 * The SubqueryAlias node keeps track of the qualifier using the information in this structure
 * @param identifier - Is an alias name or a table name
 * @param database - Is a database name and is optional
 */
case class AliasIdentifier(identifier: String, database: Option[String])
  extends IdentifierWithDatabase {

  def this(identifier: String) = this(identifier, None)
}

object AliasIdentifier {
  def apply(identifier: String): AliasIdentifier = new AliasIdentifier(identifier)
}

object CatalogTableIdentifier {
  def apply(table: String): CatalogTableIdentifier =
    new CatalogTableIdentifier(table, None, None)

  def apply(table: String, database: String): CatalogTableIdentifier =
    new CatalogTableIdentifier(table, Some(database), None)

  def apply(table: String, database: String, catalog: String): CatalogTableIdentifier =
    new CatalogTableIdentifier(table, Some(database), Some(catalog))
}

/**
 * Identifies a table in a database and catalog.
 * If `database` is not defined, the current catalog's default database is used.
 * If `catalog` is not defined, the current catalog is used.
 */
case class CatalogTableIdentifier(table: String, database: Option[String], catalog: Option[String])
    extends IdentifierWithOptionalDatabaseAndCatalog {

  // ensure database is present if catalog is defined
  assert(database.isDefined || catalog.isEmpty)

  override val identifier: String = table

  /**
   * Returns this as a TableIdentifier if its catalog is not set, fail otherwise.
   *
   * This is used to provide TableIdentifier for paths that do not support the catalog element. To
   * ensure that the identifier is compatible, this asserts that the catalog element is not defined.
   */
  lazy val asTableIdentifier: TableIdentifier = {
    assert(catalog.isEmpty, s"Cannot convert to TableIdentifier: catalog is ${catalog.get} != None")
    new TableIdentifier(table, database)
  }

  /**
   * Returns this CatalogTableIdentifier without the catalog.
   *
   * This is used for code paths where the catalog has already been used.
   */
  lazy val dropCatalog: CatalogTableIdentifier = catalog match {
    case Some(_) => CatalogTableIdentifier(table, database, None)
    case _ => this
  }
}


/**
 * Identifies a table in a database.
 * If `database` is not defined, the current database is used.
 *
 * This class is used instead of CatalogTableIdentifier in paths that do not yet support table
 * identifiers with catalogs.
 */
class TableIdentifier(table: String, db: Option[String])
    extends CatalogTableIdentifier(table, db, None) {

  def this(table: String) = this(table, None)

  override lazy val asTableIdentifier: TableIdentifier = this

  override def copy(
      name: String = this.table,
      database: Option[String] = this.db,
      catalog: Option[String] = None): TableIdentifier = {
    assert(catalog.isEmpty, "Cannot add catalog to a TableIdentifier using copy")
    new TableIdentifier(name, database)
  }
}

/** A fully qualified identifier for a table (i.e., database.tableName) */
case class QualifiedTableName(database: String, name: String) {
  override def toString: String = s"$database.$name"
}

object TableIdentifier {
  def apply(table: String): TableIdentifier =
    new TableIdentifier(table)

  def apply(table: String, database: Option[String]): TableIdentifier =
    new TableIdentifier(table, database)
}


/**
 * Identifies a function in a database.
 * If `database` is not defined, the current database is used.
 * When we register a permanent function in the FunctionRegistry, we use
 * unquotedString as the function name.
 */
case class FunctionIdentifier(funcName: String, database: Option[String])
  extends IdentifierWithOptionalDatabaseAndCatalog {

  override val identifier: String = funcName

  override val catalog: Option[String] = None

  def this(funcName: String) = this(funcName, None)

  override def toString: String = unquotedString
}

object FunctionIdentifier {
  def apply(funcName: String): FunctionIdentifier = new FunctionIdentifier(funcName)
}
