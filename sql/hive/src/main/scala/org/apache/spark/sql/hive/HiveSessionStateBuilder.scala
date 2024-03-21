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

package org.apache.spark.sql.hive

import java.lang.reflect.InvocationTargetException

import scala.util.control.NonFatal

import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDF, GenericUDTF}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EvalSubqueriesForTimeTravel, ReplaceCharWithVarchar, ResolveSessionCatalog}
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalogWithListener, InvalidUDFClassException}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.aggregate.ResolveEncodersInScalaAgg
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.execution.command.CommandCheck
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.TableCapabilityCheck
import org.apache.spark.sql.execution.streaming.ResolveWriteToStream
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.execution.PruneHiveTablePartitions
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionResourceLoader, SessionState, SparkUDFExpressionBuilder}
import org.apache.spark.util.Utils

/**
 * Builder that produces a Hive-aware `SessionState`.
 */
class HiveSessionStateBuilder(
    session: SparkSession,
    parentState: Option[SessionState])
  extends BaseSessionStateBuilder(session, parentState) {

  private def externalCatalog: ExternalCatalogWithListener = session.sharedState.externalCatalog

  /**
   * Create a Hive aware resource loader.
   */
  override protected lazy val resourceLoader: HiveSessionResourceLoader = {
    new HiveSessionResourceLoader(
      session, () => externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client)
  }

  /**
   * Create a [[HiveSessionCatalog]].
   */
  override protected lazy val catalog: HiveSessionCatalog = {
    val catalog = new HiveSessionCatalog(
      () => externalCatalog,
      () => session.sharedState.globalTempViewManager,
      new HiveMetastoreCatalog(session),
      functionRegistry,
      tableFunctionRegistry,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader,
      HiveUDFExpressionBuilder)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  /**
   * A logical query plan `Analyzer` with rules specific to Hive.
   */
  override protected def analyzer: Analyzer = new Analyzer(catalogManager) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new ResolveHiveSerdeTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        new FallBackFileSourceV2(session) +:
        ResolveEncodersInScalaAgg +:
        new ResolveSessionCatalog(catalogManager) +:
        ResolveWriteToStream +:
        new EvalSubqueriesForTimeTravel +:
        new DetermineTableStats(session) +:
        customResolutionRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      DetectAmbiguousSelfJoin +:
        RelationConversions(catalog) +:
        QualifyLocationWithWarehouse(catalog) +:
        PreprocessTableCreation(catalog) +:
        PreprocessTableInsertion +:
        DataSourceAnalysis +:
        ApplyCharTypePadding +:
        HiveAnalysis +:
        ReplaceCharWithVarchar +:
        customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        TableCapabilityCheck +:
        CommandCheck +:
        CollationCheck +:
        customCheckRules
  }

  override def customEarlyScanPushDownRules: Seq[Rule[LogicalPlan]] =
    Seq(new PruneHiveTablePartitions(session))

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override protected def planner: SparkPlanner = {
    new SparkPlanner(session, experimentalMethods) with HiveStrategies {
      override val sparkSession: SparkSession = this.session

      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies ++
          Seq(HiveTableScans, HiveScripts)
    }
  }

  override protected def newBuilder: NewBuilder = new HiveSessionStateBuilder(_, _)
}

class HiveSessionResourceLoader(
    session: SparkSession,
    clientBuilder: () => HiveClient)
  extends SessionResourceLoader(session) {
  private lazy val client = clientBuilder()
  override def addJar(path: String): Unit = {
    val uri = Utils.resolveURI(path)
    resolveJars(uri).foreach { p =>
      client.addJar(p)
      super.addJar(p)
    }
  }
}

object HiveUDFExpressionBuilder extends SparkUDFExpressionBuilder {
  override def makeExpression(name: String, clazz: Class[_], input: Seq[Expression]): Expression = {
    // Current thread context classloader may not be the one loaded the class. Need to switch
    // context classloader to initialize instance properly.
    Utils.withContextClassLoader(clazz.getClassLoader) {
      try {
        super.makeExpression(name, clazz, input)
      } catch {
        // If `super.makeFunctionExpression` throw `InvalidUDFClassException`, we construct
        // Hive UDF/UDAF/UDTF with function definition. Otherwise, we just throw it earlier.
        case _: InvalidUDFClassException =>
          makeHiveFunctionExpression(name, clazz, input)
        case NonFatal(e) => throw e
      }
    }
  }

  private def makeHiveFunctionExpression(
      name: String,
      clazz: Class[_],
      input: Seq[Expression]): Expression = {
    var udfExpr: Option[Expression] = None
    try {
      // When we instantiate hive UDF wrapper class, we may throw exception if the input
      // expressions don't satisfy the hive UDF, such as type mismatch, input number
      // mismatch, etc. Here we catch the exception and throw AnalysisException instead.
      if (classOf[UDF].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveSimpleUDF(name, new HiveFunctionWrapper(clazz.getName), input))
        udfExpr.get.dataType // Force it to check input data types.
      } else if (classOf[GenericUDF].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveGenericUDF(name, new HiveFunctionWrapper(clazz.getName), input))
        udfExpr.get.dataType // Force it to check input data types.
      } else if (classOf[AbstractGenericUDAFResolver].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveUDAFFunction(name, new HiveFunctionWrapper(clazz.getName), input))
        udfExpr.get.dataType // Force it to check input data types.
      } else if (classOf[UDAF].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveUDAFFunction(
          name,
          new HiveFunctionWrapper(clazz.getName),
          input,
          isUDAFBridgeRequired = true))
        udfExpr.get.dataType // Force it to check input data types.
      } else if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveGenericUDTF(name, new HiveFunctionWrapper(clazz.getName), input))
        // Force it to check data types.
        udfExpr.get.asInstanceOf[HiveGenericUDTF].elementSchema
      }
    } catch {
      case NonFatal(exception) =>
        val e = exception match {
          case i: InvocationTargetException => i.getCause
          case o => o
        }
        val analysisException = new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3084",
          messageParameters = Map(
            "clazz" -> clazz.getCanonicalName,
            "e" -> e.toString))
        analysisException.setStackTrace(e.getStackTrace)
        throw analysisException
    }
    udfExpr.getOrElse {
      throw QueryCompilationErrors.invalidUDFClassError(clazz.getCanonicalName)
    }
  }
}
