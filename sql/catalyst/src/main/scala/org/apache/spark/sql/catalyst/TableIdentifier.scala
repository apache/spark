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
 * Identifies a `table` in `database`.  If `database` is not defined, the current database is used.
 */
private[sql] case class TableIdentifier(table: String, database: Option[String] = None) {
  def withDatabase(database: String): TableIdentifier = this.copy(database = Some(database))

  def toSeq: Seq[String] = database.toSeq :+ table

  override def toString: String = quotedString

  def quotedString: String = toSeq.map("`" + _ + "`").mkString(".")

  def unquotedString: String = toSeq.mkString(".")
}
