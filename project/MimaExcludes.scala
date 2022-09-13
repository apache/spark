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

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.core.ProblemFilters._

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

  // Exclude rules for 3.4.x from 3.3.0
  lazy val v34excludes = defaultExcludes ++ Seq(
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.recommendation.ALS.checkedCast"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.recommendation.ALSModel.checkedCast"),

    // [SPARK-39110] Show metrics properties in HistoryServer environment tab
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.ApplicationEnvironmentInfo.this"),

    // [SPARK-38775][ML] Cleanup validation functions
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.PredictionModel.extractInstances"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.Predictor.extractInstances"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.Predictor.extractLabeledPoints"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.ClassificationModel.extractInstances"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.Classifier.extractInstances"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.Classifier.extractLabeledPoints"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.Classifier.validateNumClasses"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.Classifier.validateLabel"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.Classifier.getNumClasses"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.Classifier.getNumClasses$default$2"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.OneVsRest.extractInstances"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.OneVsRestModel.extractInstances"),

    // [SPARK-39703][SPARK-39062] Mima complains with Scala 2.13 for the changes in DeployMessages
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.deploy.DeployMessages$LaunchExecutor$"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.DeployMessages#RequestExecutors.requestedTotal"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.deploy.DeployMessages#RequestExecutors.copy"),
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.deploy.DeployMessages#RequestExecutors.copy$default$2"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.deploy.DeployMessages#RequestExecutors.this"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.deploy.DeployMessages#RequestExecutors.apply"),

    // [SPARK-38679][CORE] Expose the number of partitions in a stage to TaskContext
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.numPartitions"),

    // [SPARK-39506] In terms of 3 layer namespace effort, add currentCatalog, setCurrentCatalog and listCatalogs API to Catalog interface
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.currentCatalog"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.setCurrentCatalog"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listCatalogs"),

    // [SPARK-39704][SQL] Implement createIndex & dropIndex & indexExists in JDBC (H2 dialect)
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.jdbc.JdbcDialect.createIndex"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.jdbc.JdbcDialect.dropIndex"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.jdbc.JdbcDialect.indexExists"),

    // [SPARK-39759][SQL] Implement listIndexes in JDBC (H2 dialect)
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.jdbc.JdbcDialect.listIndexes"),

    // [SPARK-38929][SQL] Improve error messages for cast failures in ANSI
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.types.Decimal.fromStringANSI"),
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.types.Decimal.fromStringANSI$default$3"),

    // [SPARK-36511][MINOR][SQL] Remove ColumnIOUtil
    ProblemFilters.exclude[MissingClassProblem]("org.apache.parquet.io.ColumnIOUtil"),

    // [SPARK-40324][SQL] Provide query context in AnalysisException
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.AnalysisException.copy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.AnalysisException.withPosition"),

    // [SPARK-40400][SQL] Pass error message parameters to exceptions as a map
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.AnalysisException.messageParameters"),
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.AnalysisException.copy$default$7"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.AnalysisException.copy"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.AnalysisException.this"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.SparkException.this")
  )

  // Defulat exclude rules
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

    (problem: Problem) => problem match {
      case MissingClassProblem(cls) => !cls.fullName.startsWith("org.sparkproject.jpmml") &&
          !cls.fullName.startsWith("org.sparkproject.dmg.pmml")
      case _ => true
    }
  )

  def excludes(version: String) = version match {
    case v if v.startsWith("3.4") => v34excludes
    case _ => Seq()
  }
}
