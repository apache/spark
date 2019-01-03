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
package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, SupportsBatchRead, TableProvider}
import org.apache.spark.sql.types.StructType

/**
 * A base interface for data source v2 implementations of the built-in file-based data sources.
 */
trait FileDataSourceV2 extends TableProvider with DataSourceRegister {
  /**
   * Returns a V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 might be implemented partially during migration.
   *    E.g. if [[SupportsBatchRead]] is implemented while [[SupportsBatchWrite]] is not,
   *    write path should fall back to V1 implementation.
   * 2. File datasource V2 implementations cause regression.
   * 3. Catalog support is required, which is still under development for data source V2.
   */
  def fallBackFileFormat: Class[_ <: FileFormat]

  lazy val sparkSession =
    SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)

  def getFileIndex(
      options: DataSourceOptions,
      userSpecifiedSchema: Option[StructType]): PartitioningAwareFileIndex = {
    val filePaths = options.paths()
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(options.asMap().asScala.toMap)
    val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(filePaths, hadoopConf,
      checkEmptyGlobPath = false, checkFilesExist = false)
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(sparkSession, rootPathsSpecified,
      options.asMap().asScala.toMap, userSpecifiedSchema, fileStatusCache)
  }
}
