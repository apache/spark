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

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable.Map

import org.apache.commons.text.StringEscapeUtils
import org.codehaus.commons.compiler.{CompileException, InternalCompilerException}
import org.codehaus.janino.ClassBodyEvaluator

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate.newCodeGenContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{ParentClassLoader, Utils}

/**
 * A manager for user-defined data sources. It is used to register and lookup data sources by
 * their short names or fully qualified names.
 */
class DataSourceManager extends Logging {

  private type DataSourceBuilder = (
    SparkSession,  // Spark session
    String,  // provider name
    Option[StructType],  // user specified schema
    CaseInsensitiveMap[String]  // options
  ) => LogicalPlan

  // TODO(SPARK-45917): Statically load Python Data Source so idempotently Python
  //   Data Sources can be loaded even when the Driver is restarted.
  private val dataSourceBuilders = new ConcurrentHashMap[String, DataSourceBuilder]()

  private def normalize(name: String): String = name.toLowerCase(Locale.ROOT)

  /**
   * Register a data source builder for the given provider.
   * Note that the provider name is case-insensitive.
   */
  def registerDataSource(name: String, builder: DataSourceBuilder): Unit = {
    val normalizedName = normalize(name)
    val previousValue = dataSourceBuilders.put(normalizedName, builder)
    if (previousValue != null) {
      logWarning(f"The data source $name replaced a previously registered data source.")
    }
  }

  /**
   * Returns a data source builder for the given provider and throw an exception if
   * it does not exist.
   */
  def lookupDataSource(name: String): DataSourceBuilder = {
    if (dataSourceExists(name)) {
      dataSourceBuilders.get(normalize(name))
    } else {
      throw QueryCompilationErrors.dataSourceDoesNotExist(name)
    }
  }

  /**
   * Checks if a data source with the specified name exists (case-insensitive).
   */
  def dataSourceExists(name: String): Boolean = {
    dataSourceBuilders.containsKey(normalize(name))
  }

  override def clone(): DataSourceManager = {
    val manager = new DataSourceManager
    dataSourceBuilders.forEach((k, v) => manager.registerDataSource(k, v))
    manager
  }
}

/**
 * Data Source V1 default source wrapper for Python Data Source.
 */
abstract class PythonDefaultSource
    extends RelationProvider
    with SchemaRelationProvider
    with DataSourceRegister {

  override def createRelation(
      sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    new PythonRelation(shortName(), sqlContext, parameters, None)

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation =
    new PythonRelation(shortName(), sqlContext, parameters, Some(schema))
}

/**
 * Data Source V1 relation wrapper for Python Data Source.
 */
class PythonRelation(
    source: String,
    override val sqlContext: SQLContext,
    parameters: Map[String, String],
    maybeSchema: Option[StructType]) extends BaseRelation with TableScan {

  private lazy val sourceDf: DataFrame = {
    val caseInsensitiveMap = CaseInsensitiveMap(parameters)
    // TODO(SPARK-45600): should be session-based.
    val builder = sqlContext.sparkSession.sharedState.dataSourceManager.lookupDataSource(source)
    val plan = builder(
      sqlContext.sparkSession, source, caseInsensitiveMap.get("path").toSeq,
      maybeSchema, caseInsensitiveMap)
    Dataset.ofRows(sqlContext.sparkSession, plan)
  }

  override def schema: StructType = sourceDf.schema

  override def buildScan(): RDD[Row] = sourceDf.rdd
}

/**
 * Responsible for generating a class for Python Data Source
 * that inherits Scala Data Source interface so other features work together
 * with Python Data Source.
 */
object PythonDataSourceCodeGenerator extends Logging {
  /**
   * When you invoke `generateClass`, it generates a class that inherits [[PythonDefaultSource]]
   * that has a different short name. The generated class name as follows:
   * "org.apache.spark.sql.execution.datasources.$shortName.DefaultSource".
   *
   * The `shortName` should be registered via `spark.dataSource.register` first, then
   * this method can generate corresponding Scala Data Source wrapper for the Python
   * Data Source.
   *
   * @param shortName The short name registered for Python Data Source.
   * @return
   */
  def generateClass(shortName: String): Class[_] = {
    val ctx = newCodeGenContext()

    val codeBody = s"""
      @Override
      public String shortName() {
        return "${StringEscapeUtils.escapeJava(shortName)}";
      }"""

    val evaluator = new ClassBodyEvaluator()
    val parentClassLoader = new ParentClassLoader(Utils.getContextOrSparkClassLoader)
    evaluator.setParentClassLoader(parentClassLoader)
    evaluator.setClassName(
      s"org.apache.spark.sql.execution.python.datasources.$shortName.DefaultSource")
    evaluator.setExtendedClass(classOf[PythonDefaultSource])

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    // Note that the default `CodeGenerator.compile` wraps everything into a `GeneratedClass`
    // class, and the defined DataSource becomes a nested class that cannot properly define
    // getConstructors, etc. Therefore, we cannot simply reuse this.
    try {
      evaluator.cook("generated.java", code.body)
      CodeGenerator.updateAndGetCompilationStats(evaluator)
    } catch {
      case e: InternalCompilerException =>
        val msg = QueryExecutionErrors.failedToCompileMsg(e)
        logError(msg, e)
        CodeGenerator.logGeneratedCode(code)
        throw QueryExecutionErrors.internalCompilerError(e)
      case e: CompileException =>
        val msg = QueryExecutionErrors.failedToCompileMsg(e)
        logError(msg, e)
        CodeGenerator.logGeneratedCode(code)
        throw QueryExecutionErrors.compilerError(e)
    }

    logDebug(s"Generated Python Data Source':\n${CodeFormatter.format(code)}")
    evaluator.getClazz()
  }
}
