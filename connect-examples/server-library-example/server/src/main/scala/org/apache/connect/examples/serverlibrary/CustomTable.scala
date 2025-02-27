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

package org.apache.connect.examples.serverlibrary

import java.util.UUID

import com.github.mrpowers.spark.daria.sql.DariaWriters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Represents a custom table with an identifier and a DataFrame.
 *
 * @param identifier The unique identifier for the table.
 * @param df The DataFrame associated with the table.
 */
class CustomTable private (identifier: CustomTable.Identifier, df: Dataset[Row]) {

  /**
   * Returns the DataFrame associated with the table.
   *
   * @return The DataFrame.
   */
  def toDF(): Dataset[Row] = df

  /**
   * Writes the DataFrame to disk as a CSV file.
   */
  def flush(): Unit = {
    // Write dataset to disk as a CSV file
    DariaWriters.writeSingleFile(
      df = df,
      format = "csv",
      sc = df.sparkSession.sparkContext,
      tmpFolder = s"./${UUID.randomUUID().toString}",
      filename = identifier.path,
      saveMode = "overwrite")
  }
}

object CustomTable {

  /**
   * Represents the unique identifier for a custom table.
   *
   * @param name The name of the table.
   * @param path The path where the table is stored.
   */
  private case class Identifier(name: String, path: String)

  // Collection holding all the CustomTable instances and searchable by the identifier
  private val tablesByIdentifier = scala.collection.mutable.Map[Identifier, CustomTable]()

  /**
   * Creates a new custom table.
   *
   * @param name The name of the table.
   * @param path The path where the table is stored.
   * @param spark The SparkSession instance.
   * @param schema The schema of the table.
   * @return The created CustomTable instance.
   */
  private[serverlibrary] def createTable(
      name: String,
      path: String,
      spark: SparkSession,
      schema: StructType): CustomTable = {
    val identifier = Identifier(name, path)
    val df = spark.read
      .option("header", "true")
      .schema(schema)
      .csv(path)
    val table = new CustomTable(identifier, df)
    tablesByIdentifier(identifier) = table
    table
  }

  /**
   * Clones an existing custom table.
   *
   * @param sourceTable The source table to clone.
   * @param newName The name of the new table.
   * @param newPath The path where the new table will be stored.
   * @param replace Whether to replace the existing table if it exists.
   * @return The cloned CustomTable instance.
   */
  private[serverlibrary] def cloneTable(
      sourceTable: CustomTable,
      newName: String,
      newPath: String,
      replace: Boolean): CustomTable = {
    val newIdentifier = Identifier(newName, newPath)
    val clonedDf = sourceTable.toDF()
    val clonedTable = new CustomTable(newIdentifier, clonedDf)
    clonedTable.flush()
    tablesByIdentifier(newIdentifier) = clonedTable
    clonedTable
  }

  /**
   * Retrieves a custom table based on its identifier.
   *
   * @param name The name of the table.
   * @param path The path where the table is stored.
   * @return The CustomTable instance.
   * @throws IllegalArgumentException if the table is not found.
   */
  def getTable(name: String, path: String): CustomTable = {
    val identifier = Identifier(name, path)
    tablesByIdentifier.get(identifier) match {
      case Some(table) => table
      case None =>
        throw new IllegalArgumentException(s"Table with identifier $identifier not found")
    }
  }
}
