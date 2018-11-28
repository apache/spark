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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types._


object DataSourceUtils {

  /**
   * Verify if the schema is supported in datasource in write path.
   */
  def verifyWriteSchema(format: FileFormat, schema: StructType): Unit = {
    verifySchema(format, schema, isReadPath = false)
  }

  /**
   * Verify if the schema is supported in datasource in read path.
   */
  def verifyReadSchema(format: FileFormat, schema: StructType): Unit = {
    verifySchema(format, schema, isReadPath = true)
  }

  /**
   * Verify if the schema is supported in datasource. This verification should be done
   * in a driver side.
   */
  private def verifySchema(format: FileFormat, schema: StructType, isReadPath: Boolean): Unit = {
    schema.foreach { field =>
      if (!format.supportDataType(field.dataType, isReadPath)) {
        throw new AnalysisException(
          s"$format data source does not support ${field.dataType.catalogString} data type.")
      }
    }
  }

  // SPARK-24626: Metadata files and temporary files should not be
  // counted as data files, so that they shouldn't participate in tasks like
  // location size calculation.
  private[sql] def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !(name.startsWith("_") || name.startsWith("."))
  }
}
