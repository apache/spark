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

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Test DataSource that mimics Cobrix behavior by eagerly checking file existence in buildScan().
 * This is used to test that recache fails when files are missing during physical planning.
 */
class EagerTableScanRelation(
    val dataPath: String,
    @transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  override def schema: StructType = StructType(Seq(
    StructField("value", StringType, nullable = false)
  ))

  override def buildScan(): RDD[Row] = {
    // EAGERLY check if file/directory exists
    val path = new Path(dataPath)
    val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    val fs = path.getFileSystem(hadoopConf)

    if (!fs.exists(path)) {
      throw new IllegalArgumentException(
        s"EagerTableScan: File or directory does not exist: $dataPath")
    }

    // Read the actual data (simple text file)
    val textRdd = sqlContext.sparkContext.textFile(dataPath)
    textRdd.map(line => Row(line.trim))
  }
}

/**
 * RelationProvider for the eager table scan data source.
 * Usage: spark.read.format("eager-scan").option("path", "/path/to/data").load()
 */
class DefaultSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path",
      throw new IllegalArgumentException("'path' parameter is required for eager-scan format"))
    new EagerTableScanRelation(path, sqlContext)
  }
}
