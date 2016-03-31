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
  val name: String
  def database: Option[String]
  def quotedString: String = database.map(db => s"`$db`.`$name`").getOrElse(s"`$name`")
  def unquotedString: String = database.map(db => s"$db.$name").getOrElse(name)
  override def toString: String = quotedString
}


/**
 * Identifies a table in a database.
 * If `database` is not defined, the current database is used.
 */
case class TableIdentifier(table: String, database: Option[String])
  extends IdentifierWithDatabase {

  override val name: String = table

  def this(name: String) = this(name, None)

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

  override val name: String = funcName

  def this(name: String) = this(name, None)
}

object FunctionIdentifier {
  def apply(funcName: String): FunctionIdentifier = new FunctionIdentifier(funcName)
}
