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

package org.apache.spark.sql.api.python

import java.io.InputStream
import java.nio.channels.Channels

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.PythonRDDServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.types.DataType

private[sql] object PythonSQLUtils {
  def parseDataType(typeText: String): DataType = CatalystSqlParser.parseDataType(typeText)

  // This is needed when generating SQL documentation for built-in functions.
  def listBuiltinFunctionInfos(): Array[ExpressionInfo] = {
    FunctionRegistry.functionSet.flatMap(f => FunctionRegistry.builtin.lookupFunction(f)).toArray
  }

  /**
   * Python callable function to read a file in Arrow stream format and create a [[RDD]]
   * using each serialized ArrowRecordBatch as a partition.
   */
  def readArrowStreamFromFile(sqlContext: SQLContext, filename: String): JavaRDD[Array[Byte]] = {
    ArrowConverters.readArrowStreamFromFile(sqlContext, filename)
  }

  /**
   * Python callable function to read a file in Arrow stream format and create a [[DataFrame]]
   * from an RDD.
   */
  def toDataFrame(
      arrowBatchRDD: JavaRDD[Array[Byte]],
      schemaString: String,
      sqlContext: SQLContext): DataFrame = {
    ArrowConverters.toDataFrame(arrowBatchRDD, schemaString, sqlContext)
  }
}

/**
 * Helper for making a dataframe from arrow data from data sent from python over a socket.  This is
 * used when encryption is enabled, and we don't want to write data to a file.
 */
private[sql] class ArrowRDDServer(sqlContext: SQLContext) extends PythonRDDServer {

  override protected def streamToRDD(input: InputStream): RDD[Array[Byte]] = {
    // Create array to consume iterator so that we can safely close the inputStream
    val batches = ArrowConverters.getBatchesFromStream(Channels.newChannel(input)).toArray
    // Parallelize the record batches to create an RDD
    JavaRDD.fromRDD(sqlContext.sparkContext.parallelize(batches, batches.length))
  }

}
