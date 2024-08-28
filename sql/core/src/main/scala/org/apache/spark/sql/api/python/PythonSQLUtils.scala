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
import java.net.Socket
import java.nio.channels.Channels

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.spark.api.python.DechunkedInputStream
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.CLASS_LOADER
import org.apache.spark.security.SocketAuthServer
import org.apache.spark.sql.{internal, Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.{ExplainMode, QueryExecution}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.ExpressionUtils.{column, expression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.{MutableURLClassLoader, Utils}

private[sql] object PythonSQLUtils extends Logging {
  private def withInternalRowPickler(f: Pickler => Array[Byte]): Array[Byte] = {
    EvaluatePython.registerPicklers()
    val pickler = new Pickler(true, false)
    val ret = try {
        f(pickler)
      } finally {
        pickler.close()
      }
    ret
  }

  private def withInternalRowUnpickler(f: Unpickler => Any): Any = {
    EvaluatePython.registerPicklers()
    val unpickler = new Unpickler
    val ret = try {
        f(unpickler)
      } finally {
        unpickler.close()
      }
    ret
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

  def isTimestampNTZPreferred: Boolean =
    SQLConf.get.timestampType == org.apache.spark.sql.types.TimestampNTZType

  /**
   * Python callable function to read a file in Arrow stream format and create an iterator
   * of serialized ArrowRecordBatches.
   */
  def readArrowStreamFromFile(filename: String): Iterator[Array[Byte]] = {
    ArrowConverters.readArrowStreamFromFile(filename).iterator
  }

  /**
   * Python callable function to read a file in Arrow stream format and create a [[DataFrame]]
   * from the Arrow batch iterator.
   */
  def toDataFrame(
      arrowBatches: Iterator[Array[Byte]],
      schemaString: String,
      session: SparkSession): DataFrame = {
    ArrowConverters.toDataFrame(arrowBatches, schemaString, session)
  }

  def explainString(queryExecution: QueryExecution, mode: String): String = {
    queryExecution.explainString(ExplainMode.fromString(mode))
  }

  def toPyRow(row: Row): Array[Byte] = {
    assert(row.isInstanceOf[GenericRowWithSchema])
    withInternalRowPickler(_.dumps(EvaluatePython.toJava(
      CatalystTypeConverters.convertToCatalyst(row), row.schema)))
  }

  def toJVMRow(
      arr: Array[Byte],
      returnType: StructType,
      deserializer: ExpressionEncoder.Deserializer[Row]): Row = {
    val fromJava = EvaluatePython.makeFromJava(returnType)
    val internalRow =
        fromJava(withInternalRowUnpickler(_.loads(arr))).asInstanceOf[InternalRow]
    deserializer(internalRow)
  }

  /**
   * Internal-only helper for Spark Connect's local mode. This is only used for
   * local development, not for production. This method should not be used in
   * production code path.
   */
  def addJarToCurrentClassLoader(path: String): Unit = {
    Utils.getContextOrSparkClassLoader match {
      case cl: MutableURLClassLoader => cl.addURL(Utils.resolveURI(path).toURL)
      case cl => logWarning(log"Unsupported class loader ${MDC(CLASS_LOADER, cl)} will not " +
        log"update jars in the thread class loader.")
    }
  }

  def castTimestampNTZToLong(c: Column): Column =
    Column.internalFn("timestamp_ntz_to_long", c)

  def ewm(e: Column, alpha: Double, ignoreNA: Boolean): Column =
    Column.internalFn("ewm", e, lit(alpha), lit(ignoreNA))

  def nullIndex(e: Column): Column = Column.internalFn("null_index", e)

  def collect_top_k(e: Column, num: Int, reverse: Boolean): Column =
    Column.internalFn("collect_top_k", e, lit(num), lit(reverse))

  def pandasProduct(e: Column, ignoreNA: Boolean): Column =
    Column.internalFn("pandas_product", e, lit(ignoreNA))

  def pandasStddev(e: Column, ddof: Int): Column =
    Column.internalFn("pandas_stddev", e, lit(ddof))

  def pandasVariance(e: Column, ddof: Int): Column =
    Column.internalFn("pandas_var", e, lit(ddof))

  def pandasSkewness(e: Column): Column =
    Column.internalFn("pandas_skew", e)

  def pandasKurtosis(e: Column): Column =
    Column.internalFn("pandas_kurt", e)

  def pandasMode(e: Column, ignoreNA: Boolean): Column =
    Column.internalFn("pandas_mode", e, lit(ignoreNA))

  def pandasCovar(col1: Column, col2: Column, ddof: Int): Column =
    Column.internalFn("pandas_covar", col1, col2, lit(ddof))

  def unresolvedNamedLambdaVariable(name: String): Column =
    Column(internal.UnresolvedNamedLambdaVariable.apply(name))

  @scala.annotation.varargs
  def lambdaFunction(function: Column, variables: Column*): Column = {
    val arguments = variables.map(_.node.asInstanceOf[internal.UnresolvedNamedLambdaVariable])
    Column(internal.LambdaFunction(function.node, arguments))
  }

  def namedArgumentExpression(name: String, e: Column): Column = NamedArgumentExpression(name, e)

  def distributedIndex(): Column = {
    val expr = MonotonicallyIncreasingID()
    expr.setTagValue(FunctionRegistry.FUNC_ALIAS, "distributed_index")
    expr
  }

  @scala.annotation.varargs
  def fn(name: String, arguments: Column*): Column = Column.fn(name, arguments: _*)
}

/**
 * Helper for making a dataframe from Arrow data from data sent from python over a socket. This is
 * used when encryption is enabled, and we don't want to write data to a file.
 */
private[spark] class ArrowIteratorServer
  extends SocketAuthServer[Iterator[Array[Byte]]]("pyspark-arrow-batches-server") {

  def handleConnection(sock: Socket): Iterator[Array[Byte]] = {
    val in = sock.getInputStream()
    val dechunkedInput: InputStream = new DechunkedInputStream(in)
    // Create array to consume iterator so that we can safely close the file
    ArrowConverters.getBatchesFromStream(Channels.newChannel(dechunkedInput)).toArray.iterator
  }
}
