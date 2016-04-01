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

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.udf.generic.{GenericUDTF, AbstractGenericUDAFResolver, GenericUDF}
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


class HiveSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    client: HiveClient,
    context: HiveContext,
    functionRegistry: FunctionRegistry,
    conf: SQLConf)
  extends SessionCatalog(externalCatalog, functionRegistry, conf) {

  override def setCurrentDatabase(db: String): Unit = {
    super.setCurrentDatabase(db)
    client.setCurrentDatabase(db)
  }

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan = {
    val table = formatTableName(name.table)
    if (name.database.isDefined || !tempTables.contains(table)) {
      val newName = name.copy(table = table)
      metastoreCatalog.lookupRelation(newName, alias)
    } else {
      val relation = tempTables(table)
      val tableWithQualifiers = SubqueryAlias(table, relation)
      // If an alias was specified by the lookup, wrap the plan in a subquery so that
      // attributes are properly qualified with this alias.
      alias.map(a => SubqueryAlias(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
    }
  }

  // ----------------------------------------------------------------
  // | Methods and fields for interacting with HiveMetastoreCatalog |
  // ----------------------------------------------------------------

  override def getDefaultDBPath(db: String): String = {
    val defaultPath = context.hiveconf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE)
    new Path(new Path(defaultPath), db + ".db").toString
  }

  // Catalog for handling data source tables. TODO: This really doesn't belong here since it is
  // essentially a cache for metastore tables. However, it relies on a lot of session-specific
  // things so it would be a lot of work to split its functionality between HiveSessionCatalog
  // and HiveCatalog. We should still do it at some point...
  private val metastoreCatalog = new HiveMetastoreCatalog(client, context)

  val ParquetConversions: Rule[LogicalPlan] = metastoreCatalog.ParquetConversions
  val CreateTables: Rule[LogicalPlan] = metastoreCatalog.CreateTables
  val PreInsertionCasts: Rule[LogicalPlan] = metastoreCatalog.PreInsertionCasts

  override def refreshTable(name: TableIdentifier): Unit = {
    metastoreCatalog.refreshTable(name)
  }

  def invalidateTable(name: TableIdentifier): Unit = {
    metastoreCatalog.invalidateTable(name)
  }

  def invalidateCache(): Unit = {
    metastoreCatalog.cachedDataSourceTables.invalidateAll()
  }

  def createDataSourceTable(
      name: TableIdentifier,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      bucketSpec: Option[BucketSpec],
      provider: String,
      options: Map[String, String],
      isExternal: Boolean): Unit = {
    metastoreCatalog.createDataSourceTable(
      name, userSpecifiedSchema, partitionColumns, bucketSpec, provider, options, isExternal)
  }

  def hiveDefaultTableFilePath(name: TableIdentifier): String = {
    metastoreCatalog.hiveDefaultTableFilePath(name)
  }

  // For testing only
  private[hive] def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan = {
    val key = metastoreCatalog.getQualifiedTableName(table)
    metastoreCatalog.cachedDataSourceTables.getIfPresent(key)
  }

  override def makeFunctionBuilder(funcName: String, funcClassName: String): FunctionBuilder = {
    makeFunctionBuilder(funcName, Utils.classForName(funcClassName))
  }

  /**
   * Construct a [[FunctionBuilder]] based on the provided class that represents a function.
   */
  private def makeFunctionBuilder(name: String, clazz: Class[_]): FunctionBuilder = {
    // When we instantiate hive UDF wrapper class, we may throw exception if the input
    // expressions don't satisfy the hive UDF, such as type mismatch, input number
    // mismatch, etc. Here we catch the exception and throw AnalysisException instead.
    (children: Seq[Expression]) => {
      try {
        if (classOf[UDF].isAssignableFrom(clazz)) {
          val udf = HiveSimpleUDF(name, new HiveFunctionWrapper(clazz.getName), children)
          udf.dataType // Force it to check input data types.
          udf
        } else if (classOf[GenericUDF].isAssignableFrom(clazz)) {
          val udf = HiveGenericUDF(name, new HiveFunctionWrapper(clazz.getName), children)
          udf.dataType // Force it to check input data types.
          udf
        } else if (classOf[AbstractGenericUDAFResolver].isAssignableFrom(clazz)) {
          val udaf = HiveUDAFFunction(name, new HiveFunctionWrapper(clazz.getName), children)
          udaf.dataType // Force it to check input data types.
          udaf
        } else if (classOf[UDAF].isAssignableFrom(clazz)) {
          val udaf = HiveUDAFFunction(
            name,
            new HiveFunctionWrapper(clazz.getName),
            children,
            isUDAFBridgeRequired = true)
          udaf.dataType  // Force it to check input data types.
          udaf
        } else if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
          val udtf = HiveGenericUDTF(name, new HiveFunctionWrapper(clazz.getName), children)
          udtf.elementTypes // Force it to check input data types.
          udtf
        } else {
          throw new AnalysisException(s"No handler for UDF '${clazz.getCanonicalName}'")
        }
      } catch {
        case ae: AnalysisException =>
          throw ae
        case NonFatal(e) =>
          val analysisException =
            new AnalysisException(s"No handler for UDF '${clazz.getCanonicalName}': $e")
          analysisException.setStackTrace(e.getStackTrace)
          throw analysisException
      }
    }
  }
}
