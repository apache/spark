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

package org.apache.spark.sql.internal

import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalog.{Catalog, Database}

/**
 * Trait that provides DDL-based implementations of selected [[Catalog]] methods (e.g. via SQL DDL
 * such as DESCRIBE NAMESPACE EXTENDED, SHOW NAMESPACES). Core and Connect Catalog classes mix in
 * this trait and call `super` for those methods when `spark.sql.catalog.useDDLBasedAPI` is true.
 * Methods not implemented here are provided by the concrete class.
 */
private[sql] trait DDLBasedCatalog extends Catalog {

  /** Used for legacy single-part resolution. */
  private val defaultCatalogName: String = "spark_catalog"

  private def sql(str: String): DataFrame = sql(str, Map.empty)
  private[sql] def sql(str: String, args: Map[String, Any]): DataFrame
  private[sql] def parseMultipartIdentifier(identifier: String): Seq[String]
  private[sql] def quoteIdentifier(identifier: String): String

  @throws[AnalysisException]("database does not exist")
  override def getDatabase(dbName: String): Database = {
    val idents = parseMultipartIdentifier(dbName)
    // Legacy behavior: for a single-part name, prefer default catalog if it has that database.
    val identsToDescribe = if (idents.length == 1) {
      val qualified = s"${quoteIdentifier(defaultCatalogName)}.${quoteIdentifier(idents.head)}"
      if (databaseExists(qualified)) {
        Seq(defaultCatalogName, idents.head)
      } else {
        currentCatalog() +: idents
      }
    } else {
      idents
    }
    // Use parameterized SQL with identifier(:ns) so the namespace is bound as a value, not
    // concatenated into the SQL string (prevents SQL injection).
    val nsParam = identsToDescribe.mkString(".")
    val rows = sql("DESCRIBE NAMESPACE EXTENDED identifier(:ns)", Map("ns" -> nsParam)).collect()
    val info = rows.map(row => (row.getString(0), Option(row.getString(1)).getOrElse(""))).toMap
    val name = info.getOrElse("Namespace Name", identsToDescribe.last)
    val catalog = info.getOrElse(
      "Catalog Name",
      if (identsToDescribe.length > 1) identsToDescribe.head else "")
    new Database(
      name = name,
      catalog = catalog,
      description = info.get("Comment").orNull,
      locationUri = info.get("Location").orNull)
  }

  override def databaseExists(dbName: String): Boolean = {
    try {
      val parts =
        try parseMultipartIdentifier(dbName)
        catch { case NonFatal(_) => Seq(dbName) }
      if (parts.length == 1) {
        val name = parts.head
        // Legacy: resolve using existence (default first, else current); we do two explicit checks.
        def inCatalog(catalogParam: Option[String]): Boolean = {
          try {
            val (sqlStr, args) = catalogParam match {
              case Some(catalog) =>
                ("SHOW NAMESPACES IN identifier(:catalog)", Map("catalog" -> catalog))
              case None =>
                ("SHOW NAMESPACES", Map.empty[String, Any])
            }
            val rows = sql(sqlStr, args).collect()
            rows.exists(row => row.getString(0).equalsIgnoreCase(name))
          } catch {
            case _: AnalysisException => false
            case NonFatal(_) => false
          }
        }
        inCatalog(Some(defaultCatalogName)) || inCatalog(None)
      } else {
        // Multi-part: namespace can be nested (e.g. cat.ns.db). One existence check via DESCRIBE.
        try {
          val ns = parts.mkString(".")
          sql("DESCRIBE NAMESPACE EXTENDED identifier(:ns)", Map("ns" -> ns)).collect()
          true
        } catch {
          case _: AnalysisException => false
          case NonFatal(_) => false
        }
      }
    } catch {
      case _: AnalysisException => false
      case NonFatal(_) => false
    }
  }
}
