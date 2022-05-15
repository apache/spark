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
import java.util.Locale

import net.razorvine.pickle.Pickler

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.PythonRDDServer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.{ExplainMode, QueryExecution}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

private[sql] object PythonSQLUtils extends Logging {
  private lazy val internalRowPickler = {
    EvaluatePython.registerPicklers()
    new Pickler(true, false)
  }

  def parseDataType(typeText: String): DataType = CatalystSqlParser.parseDataType(typeText)

  // This is needed when generating SQL documentation for built-in functions.
  def listBuiltinFunctionInfos(): Array[ExpressionInfo] = {
    FunctionRegistry.functionSet.flatMap(f => FunctionRegistry.builtin.lookupFunction(f)).toArray
  }

  private def listAllSQLConfigs(): Seq[(String, String, String, String)] = {
    val conf = new SQLConf()
    conf.getAllDefinedConfs
  }

  def listRuntimeSQLConfigs(): Array[(String, String, String, String)] = {
    // Py4J doesn't seem to translate Seq well, so we convert to an Array.
    listAllSQLConfigs().filterNot(p => SQLConf.isStaticConfigKey(p._1)).toArray
  }

  def listStaticSQLConfigs(): Array[(String, String, String, String)] = {
    listAllSQLConfigs().filter(p => SQLConf.isStaticConfigKey(p._1)).toArray
  }

  /**
   * Python callable function to read a file in Arrow stream format and create a [[RDD]]
   * using each serialized ArrowRecordBatch as a partition.
   */
  def readArrowStreamFromFile(session: SparkSession, filename: String): JavaRDD[Array[Byte]] = {
    ArrowConverters.readArrowStreamFromFile(session, filename)
  }

  /**
   * Python callable function to read a file in Arrow stream format and create a [[DataFrame]]
   * from an RDD.
   */
  def toDataFrame(
      arrowBatchRDD: JavaRDD[Array[Byte]],
      schemaString: String,
      session: SparkSession): DataFrame = {
    ArrowConverters.toDataFrame(arrowBatchRDD, schemaString, session)
  }

  def explainString(queryExecution: QueryExecution, mode: String): String = {
    queryExecution.explainString(ExplainMode.fromString(mode))
  }

  def toPyRow(row: Row): Array[Byte] = {
    assert(row.isInstanceOf[GenericRowWithSchema])
    internalRowPickler.dumps(EvaluatePython.toJava(
      CatalystTypeConverters.convertToCatalyst(row), row.schema))
  }

  def castTimestampNTZToLong(c: Column): Column = Column(CastTimestampNTZToLong(c.expr))

  def ewm(e: Column, alpha: Double, ignoreNA: Boolean): Column =
    Column(EWM(e.expr, alpha, ignoreNA))

  def lastNonNull(e: Column): Column = Column(LastNonNull(e.expr))

  def nullIndex(e: Column): Column = Column(NullIndex(e.expr))

  def makeInterval(unit: String, e: Column): Column = {
    val zero = MakeInterval(years = Literal(0), months = Literal(0), weeks = Literal(0),
      days = Literal(0), hours = Literal(0), mins = Literal(0), secs = Literal(0))

    unit.toUpperCase(Locale.ROOT) match {
      case "YEAR" => Column(zero.copy(years = e.expr))
      case "MONTH" => Column(zero.copy(months = e.expr))
      case "WEEK" => Column(zero.copy(weeks = e.expr))
      case "DAY" => Column(zero.copy(days = e.expr))
      case "HOUR" => Column(zero.copy(hours = e.expr))
      case "MINUTE" => Column(zero.copy(mins = e.expr))
      case "SECOND" => Column(zero.copy(secs = e.expr))
      case _ => throw new IllegalStateException(s"Got the unexpected unit '$unit'.")
    }
  }

  def timestampDiff(unit: String, start: Column, end: Column): Column = {
    Column(TimestampDiff(unit, start.expr, end.expr))
  }
}

/**
 * Helper for making a dataframe from arrow data from data sent from python over a socket.  This is
 * used when encryption is enabled, and we don't want to write data to a file.
 */
private[sql] class ArrowRDDServer(session: SparkSession) extends PythonRDDServer {

  override protected def streamToRDD(input: InputStream): RDD[Array[Byte]] = {
    // Create array to consume iterator so that we can safely close the inputStream
    val batches = ArrowConverters.getBatchesFromStream(Channels.newChannel(input)).toArray
    // Parallelize the record batches to create an RDD
    JavaRDD.fromRDD(session.sparkContext.parallelize(batches, batches.length))
  }

}
