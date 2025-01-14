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

package org.example

import java.util.UUID
import com.github.mrpowers.spark.daria.sql.DariaWriters
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

class CustomTable private (identifier: CustomTable.Identifier, df: Dataset[Row]) {

  def toDF(): Dataset[Row] = df
  def flush(): Unit = {
    // write dataset to disk as csv file
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

  private case class Identifier(name: String, path: String)
  // Collection holding all the CustomTable instances and searchable by the identifer
  private val tablesByIdentifier = scala.collection.mutable.Map[Identifier, CustomTable]()

  private[example] def createTable(
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

  private[example] def cloneTable(
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

  // Factory method to get a CustomTable based on it's identifer
  def getTable(name: String, path: String): CustomTable = {
    val identifier = Identifier(name, path)
    tablesByIdentifier.get(identifier) match {
      case Some(table) => table
      case None =>
        throw new IllegalArgumentException(s"Table with identifier $identifier not found")
    }
  }
}
