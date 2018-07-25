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

package org.apache.spark.sql.execution.datasources

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_FILE_INDEX_IMPLEMENTATION
import org.apache.spark.util.Utils

/**
 * A [[FileIndex]] for a metastore catalog table.
 */
trait CatalogFileIndex extends FileIndex {

  /**
   * Returns a [[FileIndex]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters.
   *
   * @param filters partition-pruning filters
   */
  def filterPartitions(filters: Seq[Expression]): FileIndex

}

trait CatalogFileIndexFactory {

  /**
   * Creates [[CatalogFileIndex]] for given table
   */
  def create(
    spark: SparkSession,
    catalogTable: CatalogTable,
    tableSize: Long): CatalogFileIndex

}

object CatalogFileIndexFactory {

  def reflect[T <: CatalogFileIndexFactory](conf: SparkConf): T = {
    val className = fileIndexClassName(conf)
    try {
      val ctor = Utils.classForName(className).getDeclaredConstructor()
      ctor.newInstance().asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  private def fileIndexClassName(conf: SparkConf): String = {
    conf.get(CATALOG_FILE_INDEX_IMPLEMENTATION) match {
      case "hive" => "org.apache.spark.sql.execution.datasources.HiveCatalogFileIndexFactory"
      case name => name
    }
  }

}
