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

import com.typesafe.tools.mima.core
import com.typesafe.tools.mima.core.*

/**
 * Additional excludes for checking of Spark's binary compatibility.
 *
 * This acts as an official audit of cases where we excluded other classes. Please use the narrowest
 * possible exclude here. MIMA will usually tell you what exclude to use, e.g.:
 *
 * ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.take")
 *
 * It is also possible to exclude Spark classes and packages. This should be used sparingly:
 *
 * MimaBuild.excludeSparkClass("graphx.util.collection.GraphXPrimitiveKeyOpenHashMap")
 *
 * For a new Spark version, please update MimaBuild.scala to reflect the previous version.
 */
object MimaExcludes {

  // Exclude rules for 4.0.x from 3.5.0
  lazy val v40excludes = defaultExcludes ++ Seq(
    // [SPARK-44863][UI] Add a button to download thread dump as a txt in Spark UI
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.ThreadStackTrace.*"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.status.api.v1.ThreadStackTrace$"),
    //[SPARK-46399][Core] Add exit status to the Application End event for the use of Spark Listener
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerApplicationEnd.*"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.scheduler.SparkListenerApplicationEnd$"),
    // [SPARK-45427][CORE] Add RPC SSL settings to SSLOptions and SparkTransportConf
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.network.netty.SparkTransportConf.fromSparkConf"),
    // [SPARK-45022][SQL] Provide context for dataset API errors
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.QueryContext.contextType"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.QueryContext.code"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.QueryContext.callSite"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.QueryContext.summary"),
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.types.Decimal.fromStringANSI$default$3"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.types.Decimal.fromStringANSI"),
    // [SPARK-45762][CORE] Support shuffle managers defined in user jars by changing startup order
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkEnv.this"),
    // [SPARK-46480][CORE][SQL] Fix NPE when table cache task attempt
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.isFailed"),

    // SPARK-43299: Convert StreamingQueryException in Scala Client
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.StreamingQueryException"),

    // SPARK-45856: Move ArtifactManager from Spark Connect into SparkSession (sql/core)
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.apply"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.userId"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.sessionId"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.copy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.copy$default$3"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.this"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.storage.CacheId$"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.CacheId.apply"),

    // SPARK-46410: Assign error classes/subclasses to JdbcUtils.classifyException
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.JdbcDialect.classifyException"),
    // TODO(SPARK-46878): Invalid Mima report for StringType extension
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.types.StringType.this"),
    // SPARK-47011: Remove deprecated BinaryClassificationMetrics.scoreLabelsWeight
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.scoreLabelsWeight"),
    // SPARK-46938: Javax -> Jakarta namespace change.
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ui.ProxyRedirectHandler$ResponseWrapper"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ui.ProxyRedirectHandler#ResponseWrapper.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.DB2Dialect#DB2SQLBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.DB2Dialect#DB2SQLQueryBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.MsSqlServerDialect#MsSqlServerSQLBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.MsSqlServerDialect#MsSqlServerSQLQueryBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.MySQLDialect#MySQLSQLBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.MySQLDialect#MySQLSQLQueryBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.OracleDialect#OracleSQLBuilder.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.jdbc.OracleDialect#OracleSQLQueryBuilder.this"),
    // SPARK-47706: Bump json4s from 3.7.0-M11 to 4.0.7
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.expressions.MutableAggregationBuffer.jsonValue"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.types.DataType#JSortedObject.unapplySeq"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.tree.model.TreeEnsembleModel#SaveLoadV1_0.readMetadata"),
    // SPARK-47814: Move `KinesisTestUtils` & `WriteInputFormatTestDataGenerator` from `main` to `test`
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.python.TestWritable"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.python.TestWritable$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.python.WriteInputFormatTestDataGenerator"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.api.python.WriteInputFormatTestDataGenerator$"),
    // SPARK-47764: Cleanup shuffle dependencies based on ShuffleCleanupMode
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.shuffle.MigratableResolver.addShuffleToSkip"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLContext#implicits._sqlContext"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLImplicits._sqlContext"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.SQLImplicits.session"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SparkSession#implicits._sqlContext"),
    // SPARK-48761: Add clusterBy() to CreateTableWriter.
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.CreateTableWriter.clusterBy"),
    // SPARK-48900: Add `reason` string to all job / stage / job group cancellation calls
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.scheduler.JobWaiter.cancel"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.FutureAction.cancel"),
    // SPARK-48901: Add clusterBy() to DataStreamWriter.
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.DataStreamWriter.clusterBy")
  )

  // Default exclude rules
  lazy val defaultExcludes = Seq(
    // Spark Internals
    ProblemFilters.exclude[Problem]("org.apache.spark.rpc.*"),
    ProblemFilters.exclude[Problem]("org.spark-project.jetty.*"),
    ProblemFilters.exclude[Problem]("org.spark_project.jetty.*"),
    ProblemFilters.exclude[Problem]("org.sparkproject.jetty.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.internal.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.unused.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.unsafe.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.memory.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.util.collection.unsafe.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.catalyst.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.execution.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.internal.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.errors.*"),
    // DSv2 catalog and expression APIs are unstable yet. We should enable this back.
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.connector.catalog.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.connector.expressions.*"),
    // Avro source implementation is internal.
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.v2.avro.*"),

    // SPARK-43169: shaded and generated protobuf code
    ProblemFilters.exclude[Problem]("org.sparkproject.spark_core.protobuf.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.status.protobuf.StoreTypes*"),

    // SPARK-44104: shaded protobuf code and Apis with parameters relocated
    ProblemFilters.exclude[Problem]("org.sparkproject.spark_protobuf.protobuf.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.protobuf.utils.SchemaConverters.*"),

    (problem: Problem) => problem match {
      case MissingClassProblem(cls) => !cls.fullName.startsWith("org.sparkproject.jpmml") &&
          !cls.fullName.startsWith("org.sparkproject.dmg.pmml")
      case _ => true
    }
  )

  def excludes(version: String): Seq[Problem => Boolean] = version match {
    case v if v.startsWith("4.0") => v40excludes
    case _ => Seq()
  }
}
