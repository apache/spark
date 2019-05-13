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

  // Exclude rules for 3.0.x
  lazy val v30excludes = v24excludes ++ Seq(
    // [SPARK-27410][MLLIB] Remove deprecated / no-op mllib.KMeans getRuns, setRuns
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.KMeans.getRuns"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.KMeans.setRuns"),

    // [SPARK-27090][CORE] Removing old LEGACY_DRIVER_IDENTIFIER ("<driver>")
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkContext.LEGACY_DRIVER_IDENTIFIER"),
    // [SPARK-25838] Remove formatVersion from Saveable
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.DistributedLDAModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.LocalLDAModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.BisectingKMeansModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.KMeansModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.PowerIterationClusteringModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.GaussianMixtureModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.recommendation.MatrixFactorizationModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.feature.ChiSqSelectorModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.feature.Word2VecModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.classification.SVMModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.classification.LogisticRegressionModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.classification.NaiveBayesModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.util.Saveable.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.fpm.FPGrowthModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.fpm.PrefixSpanModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.regression.IsotonicRegressionModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.regression.RidgeRegressionModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.regression.LassoModel.formatVersion"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.regression.LinearRegressionModel.formatVersion"),

    // [SPARK-26132] Remove support for Scala 2.11 in Spark 3.0.0
    ProblemFilters.exclude[DirectAbstractMethodProblem]("scala.concurrent.Future.transformWith"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("scala.concurrent.Future.transform"),

    // [SPARK-26254][CORE] Extract Hive + Kafka dependencies from Core.
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.deploy.security.HiveDelegationTokenProvider"),

    // [SPARK-26311][CORE]New feature: apply custom log URL pattern for executor log URLs
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerApplicationStart.apply"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerApplicationStart.copy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerApplicationStart.this"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.scheduler.SparkListenerApplicationStart$"),
    
    // [SPARK-26632][Core] Separate Thread Configurations of Driver and Executor
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.network.netty.SparkTransportConf.fromSparkConf"),

    // [SPARK-25765][ML] Add training cost to BisectingKMeans summary
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.BisectingKMeansModel.this"),

    // [SPARK-24243][CORE] Expose exceptions from InProcessAppHandle
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.launcher.SparkAppHandle.getError"),

    // [SPARK-25867] Remove KMeans computeCost
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.clustering.KMeansModel.computeCost"),

    // [SPARK-26127] Remove deprecated setters from tree regression and classification models
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setSeed"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setMinInfoGain"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setCacheNodeIds"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setCheckpointInterval"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setMaxDepth"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setImpurity"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setMaxMemoryInMB"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setMaxBins"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.DecisionTreeClassificationModel.setMinInstancesPerNode"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setSeed"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setMinInfoGain"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setSubsamplingRate"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setMaxIter"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setCacheNodeIds"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setCheckpointInterval"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setMaxDepth"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setImpurity"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setMaxMemoryInMB"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setStepSize"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setMaxBins"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setMinInstancesPerNode"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.setFeatureSubsetStrategy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setSeed"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setMinInfoGain"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setSubsamplingRate"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setCacheNodeIds"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setCheckpointInterval"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setMaxDepth"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setImpurity"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setMaxMemoryInMB"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setFeatureSubsetStrategy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setMaxBins"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setMinInstancesPerNode"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setNumTrees"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setSeed"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setMinInfoGain"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setCacheNodeIds"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setCheckpointInterval"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setMaxDepth"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setImpurity"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setMaxMemoryInMB"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setMaxBins"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.DecisionTreeRegressionModel.setMinInstancesPerNode"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setSeed"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setMinInfoGain"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setSubsamplingRate"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setMaxIter"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setCacheNodeIds"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setCheckpointInterval"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setMaxDepth"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setImpurity"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setMaxMemoryInMB"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setStepSize"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setMaxBins"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setMinInstancesPerNode"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.setFeatureSubsetStrategy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setSeed"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setMinInfoGain"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setSubsamplingRate"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setCacheNodeIds"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setCheckpointInterval"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setMaxDepth"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setImpurity"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setMaxMemoryInMB"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setFeatureSubsetStrategy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setMaxBins"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setMinInstancesPerNode"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setNumTrees"),

    // [SPARK-26124] Update plugins, including MiMa
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns.build"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics.fullSchema"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics.planInputPartitions"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsReportPartitioning.fullSchema"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsReportPartitioning.planInputPartitions"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters.build"),

    // [SPARK-26090] Resolve most miscellaneous deprecation and build warnings for Spark 3
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.mllib.stat.test.BinarySampleBeanInfo"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.mllib.regression.LabeledPointBeanInfo"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ml.feature.LabeledPointBeanInfo"),

    // [SPARK-25959] GBTClassifier picks wrong impurity stats on loading
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.tree.HasVarianceImpurity.org$apache$spark$ml$tree$HasVarianceImpurity$_setter_$impurity_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.tree.HasVarianceImpurity.org$apache$spark$ml$tree$HasVarianceImpurity$_setter_$impurity_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.tree.HasVarianceImpurity.org$apache$spark$ml$tree$HasVarianceImpurity$_setter_$impurity_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.tree.HasVarianceImpurity.org$apache$spark$ml$tree$HasVarianceImpurity$_setter_$impurity_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.tree.HasVarianceImpurity.org$apache$spark$ml$tree$HasVarianceImpurity$_setter_$impurity_="),

    // [SPARK-25908][CORE][SQL] Remove old deprecated items in Spark 3
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.BarrierTaskContext.isRunningLocally"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskContext.isRunningLocally"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.shuffleBytesWritten"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.shuffleWriteTime"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.shuffleRecordsWritten"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.AccumulableInfo.apply"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.approxCountDistinct"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.toRadians"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.toDegrees"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.monotonicallyIncreasingId"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLContext.clearActive"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLContext.getOrCreate"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLContext.setActive"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.SQLContext.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.evaluation.MulticlassMetrics.fMeasure"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.evaluation.MulticlassMetrics.recall"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.evaluation.MulticlassMetrics.precision"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.util.MLWriter.context"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.util.MLReader.context"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.util.GeneralMLWriter.context"),

    // [SPARK-25737] Remove JavaSparkContextVarargsWorkaround
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.api.java.JavaSparkContext"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.api.java.JavaSparkContext.union"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.streaming.api.java.JavaStreamingContext.union"),

    // [SPARK-16775] Remove deprecated accumulator v1 APIs
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.Accumulable"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.AccumulatorParam"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.Accumulator"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.Accumulator$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.AccumulableParam"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.AccumulatorParam$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.AccumulatorParam$FloatAccumulatorParam$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.AccumulatorParam$DoubleAccumulatorParam$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.AccumulatorParam$LongAccumulatorParam$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.AccumulatorParam$IntAccumulatorParam$"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkContext.accumulable"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkContext.accumulableCollection"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkContext.accumulator"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.LegacyAccumulatorWrapper"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.api.java.JavaSparkContext.intAccumulator"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.api.java.JavaSparkContext.accumulable"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.api.java.JavaSparkContext.doubleAccumulator"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.api.java.JavaSparkContext.accumulator"),

    // [SPARK-24109] Remove class SnappyOutputStreamWrapper
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.io.SnappyCompressionCodec.version"),

    // [SPARK-19287] JavaPairRDD flatMapValues requires function returning Iterable, not Iterator
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.api.java.JavaPairRDD.flatMapValues"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.streaming.api.java.JavaPairDStream.flatMapValues"),

    // [SPARK-25680] SQL execution listener shouldn't happen on execution thread
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.util.ExecutionListenerManager.clone"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.util.ExecutionListenerManager.this"),

    // [SPARK-25862][SQL] Remove rangeBetween APIs introduced in SPARK-21608
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.unboundedFollowing"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.unboundedPreceding"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.currentRow"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.expressions.Window.rangeBetween"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.expressions.WindowSpec.rangeBetween"),

    // [SPARK-23781][CORE] Merge token renewer functionality into HadoopDelegationTokenManager
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.nextCredentialRenewalTime"),

    // [SPARK-26133][ML] Remove deprecated OneHotEncoder and rename OneHotEncoderEstimator to OneHotEncoder
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ml.feature.OneHotEncoderEstimator"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.feature.OneHotEncoder"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.OneHotEncoder.transform"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.OneHotEncoder.getInputCol"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.OneHotEncoder.getOutputCol"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.OneHotEncoder.inputCol"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.OneHotEncoder.setInputCol"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.OneHotEncoder.setOutputCol"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.OneHotEncoder.outputCol"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ml.feature.OneHotEncoderEstimator$"),

    // [SPARK-26141] Enable custom metrics implementation in shuffle write
    // Following are Java private classes
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.shuffle.sort.UnsafeShuffleWriter.this"),
    ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.storage.TimeTrackingOutputStream.this"),

    // [SPARK-26139] Implement shuffle write metrics in SQL
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ShuffleDependency.this"),

    // [SPARK-26362][CORE] Remove 'spark.driver.allowMultipleContexts' to disallow multiple creation of SparkContexts
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkContext.setActiveContext"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkContext.markPartiallyConstructed"),

    // [SPARK-26457] Show hadoop configurations in HistoryServer environment tab
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.ApplicationEnvironmentInfo.this"),

    // Data Source V2 API changes
    (problem: Problem) => problem match {
      case MissingClassProblem(cls) =>
        !cls.fullName.startsWith("org.apache.spark.sql.sources.v2")
      case MissingTypesProblem(newCls, _) =>
        !newCls.fullName.startsWith("org.apache.spark.sql.sources.v2")
      case InheritedNewAbstractMethodProblem(cls, _) =>
        !cls.fullName.startsWith("org.apache.spark.sql.sources.v2")
      case DirectMissingMethodProblem(meth) =>
        !meth.owner.fullName.startsWith("org.apache.spark.sql.sources.v2")
      case ReversedMissingMethodProblem(meth) =>
        !meth.owner.fullName.startsWith("org.apache.spark.sql.sources.v2")
      case _ => true
    },

    // [SPARK-26216][SQL] Do not use case class as public API (UserDefinedFunction)
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.expressions.UserDefinedFunction$"),
    ProblemFilters.exclude[AbstractClassProblem]("org.apache.spark.sql.expressions.UserDefinedFunction"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.inputTypes"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.nullableTypes_="),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.dataType"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.f"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.this"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.asNonNullable"),
    ProblemFilters.exclude[ReversedAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.asNonNullable"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.nullable"),
    ProblemFilters.exclude[ReversedAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.nullable"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.asNondeterministic"),
    ProblemFilters.exclude[ReversedAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.asNondeterministic"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.deterministic"),
    ProblemFilters.exclude[ReversedAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.deterministic"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.apply"),
    ProblemFilters.exclude[ReversedAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.apply"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.withName"),
    ProblemFilters.exclude[ReversedAbstractMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.withName"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.productElement"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.productArity"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.copy$default$2"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.canEqual"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.copy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.copy$default$1"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.productIterator"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.productPrefix"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.UserDefinedFunction.copy$default$3"),

    // [SPARK-11215][ML] Add multiple columns support to StringIndexer
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.StringIndexer.validateAndTransformSchema"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.StringIndexerModel.validateAndTransformSchema"),

    // [SPARK-26616][MLlib] Expose document frequency in IDFModel
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.feature.IDFModel.this"),
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.mllib.feature.IDF#DocumentFrequencyAggregator.idf")
  )

  // Exclude rules for 2.4.x
  lazy val v24excludes = v23excludes ++ Seq(
    // [SPARK-23429][CORE] Add executor memory metrics to heartbeat and expose in executors REST API
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate.apply"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate.copy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate.this"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate$"),

    // [SPARK-25248] add package private methods to TaskContext
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.markTaskFailed"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.markInterrupted"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.fetchFailed"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.markTaskCompleted"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.getLocalProperties"),

    // [SPARK-10697][ML] Add lift to Association rules
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.fpm.FPGrowthModel.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.fpm.AssociationRules#Rule.this"),

    // [SPARK-24296][CORE] Replicate large blocks as a stream.
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.network.netty.NettyBlockRpcServer.this"),
    // [SPARK-23528] Add numIter to ClusteringSummary
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.clustering.ClusteringSummary.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.clustering.KMeansSummary.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.clustering.BisectingKMeansSummary.this"),
    // [SPARK-6237][NETWORK] Network-layer changes to allow stream upload
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.network.netty.NettyBlockRpcServer.receive"),

    // [SPARK-20087][CORE] Attach accumulators / metrics to 'TaskKilled' end reason
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.apply"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.copy"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.this"),

    // [SPARK-22941][core] Do not exit JVM when submit fails with in-process launcher.
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.printWarning"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.parseSparkConfProperty"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.printVersionAndExit"),

    // [SPARK-23412][ML] Add cosine distance measure to BisectingKmeans
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasDistanceMeasure.org$apache$spark$ml$param$shared$HasDistanceMeasure$_setter_$distanceMeasure_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasDistanceMeasure.getDistanceMeasure"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasDistanceMeasure.distanceMeasure"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.BisectingKMeansModel#SaveLoadV1_0.load"),

    // [SPARK-20659] Remove StorageStatus, or make it private
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.SparkExecutorInfo.totalOffHeapStorageMemory"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.SparkExecutorInfo.usedOffHeapStorageMemory"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.SparkExecutorInfo.usedOnHeapStorageMemory"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.SparkExecutorInfo.totalOnHeapStorageMemory"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkContext.getExecutorStorageStatus"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.numBlocks"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.numRddBlocks"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.containsBlock"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.rddBlocksById"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.numRddBlocksById"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.memUsedByRdd"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.cacheSize"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.rddStorageLevel"),

    // [SPARK-23455][ML] Default Params in ML should be saved separately in metadata
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.param.Params.paramMap"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.param.Params.org$apache$spark$ml$param$Params$_setter_$paramMap_="),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.param.Params.defaultParamMap"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.param.Params.org$apache$spark$ml$param$Params$_setter_$defaultParamMap_="),

    // [SPARK-7132][ML] Add fit with validation set to spark.ml GBT
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.getValidationIndicatorCol"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.org$apache$spark$ml$param$shared$HasValidationIndicatorCol$_setter_$validationIndicatorCol_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.validationIndicatorCol"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.getValidationIndicatorCol"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.org$apache$spark$ml$param$shared$HasValidationIndicatorCol$_setter_$validationIndicatorCol_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.validationIndicatorCol"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.getValidationIndicatorCol"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.org$apache$spark$ml$param$shared$HasValidationIndicatorCol$_setter_$validationIndicatorCol_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasValidationIndicatorCol.validationIndicatorCol"),

    // [SPARK-23042] Use OneHotEncoderModel to encode labels in MultilayerPerceptronClassifier
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ml.classification.LabelConverter"),

    // [SPARK-21842][MESOS] Support Kerberos ticket renewal and creation in Mesos
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.getDateOfNextUpdate"),

    // [SPARK-23366] Improve hot reading path in ReadAheadInputStream
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.io.ReadAheadInputStream.this"),

    // [SPARK-22941][CORE] Do not exit JVM when submit fails with in-process launcher.
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.addJarToClasspath"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.mergeFileLists"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.prepareSubmitEnvironment$default$2"),

    // Data Source V2 API changes
    // TODO: they are unstable APIs and should not be tracked by mima.
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.v2.ReadSupportWithSchema"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch.createDataReaderFactories"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch.createBatchDataReaderFactories"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch.planBatchInputPartitions"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.v2.reader.SupportsScanUnsafeRow"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.sources.v2.reader.DataSourceReader.createDataReaderFactories"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.v2.reader.DataSourceReader.planInputPartitions"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.v2.reader.SupportsPushDownCatalystFilters"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.v2.reader.DataReader"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics.getStatistics"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics.estimateStatistics"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.v2.reader.DataReaderFactory"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.v2.reader.streaming.ContinuousDataReader"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.sources.v2.writer.DataWriterFactory.createDataWriter"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.v2.writer.DataWriterFactory.createDataWriter"),

    // Changes to HasRawPredictionCol.
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasRawPredictionCol.rawPredictionCol"),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasRawPredictionCol.org$apache$spark$ml$param$shared$HasRawPredictionCol$_setter_$rawPredictionCol_="),
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasRawPredictionCol.getRawPredictionCol"),

    // [SPARK-15526][ML][FOLLOWUP] Make JPMML provided scope to avoid including unshaded JARs
    (problem: Problem) => problem match {
      case MissingClassProblem(cls) =>
        !cls.fullName.startsWith("org.sparkproject.jpmml") &&
          !cls.fullName.startsWith("org.sparkproject.dmg.pmml") &&
          !cls.fullName.startsWith("org.spark_project.jpmml") &&
          !cls.fullName.startsWith("org.spark_project.dmg.pmml")
      case _ => true
    }
  )

  // Exclude rules for 2.3.x
  lazy val v23excludes = v22excludes ++ Seq(
    // [SPARK-22897] Expose stageAttemptId in TaskContext
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.stageAttemptNumber"),

    // SPARK-22789: Map-only continuous processing execution
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryManager.startQuery$default$8"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryManager.startQuery$default$6"),
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryManager.startQuery$default$9"),

    // SPARK-22372: Make cluster submission use SparkApplication.
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.getSecretKeyFromUserCredentials"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.isYarnMode"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.getCurrentUserCredentials"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.addSecretKeyToUserCredentials"),

    // SPARK-18085: Better History Server scalability for many / large applications
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.ExecutorSummary.executorLogs"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.history.HistoryServer.getSparkUI"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ui.env.EnvironmentListener"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ui.exec.ExecutorsListener"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ui.storage.StorageListener"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.storage.StorageStatusListener"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.ExecutorStageSummary.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.JobData.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkStatusTracker.this"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ui.jobs.JobProgressListener"),

    // [SPARK-20495][SQL] Add StorageLevel to cacheTable API
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.cacheTable"),

    // [SPARK-19937] Add remote bytes read to disk.
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.ShuffleReadMetrics.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.ShuffleReadMetricDistributions.this"),

    // [SPARK-21276] Update lz4-java to the latest (v1.4.0)
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.io.LZ4BlockInputStream"),

    // [SPARK-17139] Add model summary for MultinomialLogisticRegression
    ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary"),
    ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionSummary"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.predictionCol"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.labels"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.truePositiveRateByLabel"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.falsePositiveRateByLabel"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.precisionByLabel"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.recallByLabel"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.fMeasureByLabel"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.accuracy"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.weightedTruePositiveRate"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.weightedFalsePositiveRate"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.weightedRecall"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.weightedPrecision"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.weightedFMeasure"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.asBinary"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.org$apache$spark$ml$classification$LogisticRegressionSummary$$multiclassMetrics"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.org$apache$spark$ml$classification$LogisticRegressionSummary$_setter_$org$apache$spark$ml$classification$LogisticRegressionSummary$$multiclassMetrics_="),

    // [SPARK-14280] Support Scala 2.12
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.FutureAction.transformWith"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.FutureAction.transform"),

    // [SPARK-21087] CrossValidator, TrainValidationSplit expose sub models after fitting: Scala
    ProblemFilters.exclude[FinalClassProblem]("org.apache.spark.ml.tuning.CrossValidatorModel$CrossValidatorModelWriter"),
    ProblemFilters.exclude[FinalClassProblem]("org.apache.spark.ml.tuning.TrainValidationSplitModel$TrainValidationSplitModelWriter"),

    // [SPARK-21728][CORE] Allow SparkSubmit to use Logging
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.downloadFileList"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.downloadFile"),

    // [SPARK-21714][CORE][YARN] Avoiding re-uploading remote resources in yarn client mode
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkSubmit.prepareSubmitEnvironment"),

    // [SPARK-22324][SQL][PYTHON] Upgrade Arrow to 0.8.0
    ProblemFilters.exclude[FinalMethodProblem]("org.apache.spark.network.util.AbstractFileRegion.transfered"),

    // [SPARK-20643][CORE] Add listener implementation to collect app state
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskData.<init>$default$5"),

    // [SPARK-20648][CORE] Port JobsTab and StageTab to the new UI backend
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskData.<init>$default$12"),

    // [SPARK-21462][SS] Added batchId to StreamingQueryProgress.json
    // [SPARK-21409][SS] Expose state store memory usage in SQL metrics and progress updates
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StateOperatorProgress.this"),

    // [SPARK-22278][SS] Expose current event time watermark and current processing time in GroupState
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.GroupState.getCurrentWatermarkMs"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.GroupState.getCurrentProcessingTimeMs"),

    // [SPARK-20542][ML][SQL] Add an API to Bucketizer that can bin multiple columns
    ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasOutputCols.org$apache$spark$ml$param$shared$HasOutputCols$_setter_$outputCols_="),

    // [SPARK-18619][ML] Make QuantileDiscretizer/Bucketizer/StringIndexer/RFormula inherit from HasHandleInvalid
    ProblemFilters.exclude[FinalMethodProblem]("org.apache.spark.ml.feature.Bucketizer.getHandleInvalid"),
    ProblemFilters.exclude[FinalMethodProblem]("org.apache.spark.ml.feature.StringIndexer.getHandleInvalid"),
    ProblemFilters.exclude[FinalMethodProblem]("org.apache.spark.ml.feature.QuantileDiscretizer.getHandleInvalid"),
    ProblemFilters.exclude[FinalMethodProblem]("org.apache.spark.ml.feature.StringIndexerModel.getHandleInvalid")
  )

  // Exclude rules for 2.2.x
  lazy val v22excludes = v21excludes ++ Seq(
    // [SPARK-20355] Add per application spark version on the history server headerpage
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.ApplicationAttemptInfo.this"),

    // [SPARK-19652][UI] Do auth checks for REST API access.
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.history.HistoryServer.withSparkUI"),
    ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.status.api.v1.UIRootFromServletContext"),

    // [SPARK-18663][SQL] Simplify CountMinSketch aggregate implementation
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.util.sketch.CountMinSketch.toByteArray"),

    // [SPARK-18949] [SQL] Add repairTable API to Catalog
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.recoverPartitions"),

    // [SPARK-18537] Add a REST api to spark streaming
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.streaming.scheduler.StreamingListener.onStreamingStarted"),

    // [SPARK-19148][SQL] do not expose the external table concept in Catalog
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.createTable"),

    // [SPARK-14272][ML] Add logLikelihood in GaussianMixtureSummary
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.clustering.GaussianMixtureSummary.this"),

    // [SPARK-19267] Fetch Failure handling robust to user error handling
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.setFetchFailed"),

    // [SPARK-19069] [CORE] Expose task 'status' and 'duration' in spark history server REST API.
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskData.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskData.<init>$default$10"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskData.<init>$default$11"),

    // [SPARK-17161] Removing Python-friendly constructors not needed
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.OneVsRestModel.this"),

    // [SPARK-19820] Allow reason to be specified to task kill
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.TaskKilled$"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.productElement"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.productArity"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.canEqual"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.productIterator"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.countTowardsTaskFailures"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.productPrefix"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.TaskKilled.toErrorString"),
    ProblemFilters.exclude[FinalMethodProblem]("org.apache.spark.TaskKilled.toString"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.killTaskIfInterrupted"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.getKillReason"),

    // [SPARK-19876] Add one time trigger, and improve Trigger APIs
    ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.sql.streaming.Trigger"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.sql.streaming.ProcessingTime"),

    // [SPARK-17471][ML] Add compressed method to ML matrices
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.compressed"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.compressedColMajor"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.compressedRowMajor"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.isRowMajor"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.isColMajor"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.getSparseSizeInBytes"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.toDense"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.toSparse"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.toDenseRowMajor"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.toSparseRowMajor"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.toSparseColMajor"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.getDenseSizeInBytes"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.toDenseColMajor"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.toDenseMatrix"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.toSparseMatrix"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Matrix.getSizeInBytes"),

    // [SPARK-18693] Added weightSum to trait MultivariateStatisticalSummary
    ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.stat.MultivariateStatisticalSummary.weightSum")
  ) ++ Seq(
      // [SPARK-17019] Expose on-heap and off-heap memory usage in various places
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerBlockManagerAdded.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerBlockManagerAdded.this"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.scheduler.SparkListenerBlockManagerAdded$"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerBlockManagerAdded.apply"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.storage.StorageStatus.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.RDDDataDistribution.this")
    )

  // Exclude rules for 2.1.x
  lazy val v21excludes = v20excludes ++ {
    Seq(
      // [SPARK-17671] Spark 2.0 history server summary page is slow even set spark.history.ui.maxApplications
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.deploy.history.HistoryServer.getApplicationList"),
      // [SPARK-14743] Improve delegation token handling in secure cluster
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.getTimeFromNowToRenewal"),
      // [SPARK-16199][SQL] Add a method to list the referenced columns in data source Filter
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.Filter.references"),
      // [SPARK-16853][SQL] Fixes encoder error in DataSet typed select
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.Dataset.select"),
      // [SPARK-16967] Move Mesos to Module
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkMasterRegex.MESOS_REGEX"),
      // [SPARK-16240] ML persistence backward compatibility for LDA
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.clustering.LDA$"),
      // [SPARK-17717] Add Find and Exists method to Catalog.
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.getDatabase"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.getTable"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.getFunction"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.databaseExists"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.tableExists"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.functionExists"),

      // [SPARK-17731][SQL][Streaming] Metrics for structured streaming
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.SourceStatus.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.streaming.SourceStatus.offsetDesc"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQuery.status"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.SinkStatus.this"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.StreamingQueryInfo"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryListener#QueryStarted.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryListener#QueryStarted.queryInfo"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryListener#QueryProgress.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryListener#QueryProgress.queryInfo"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryListener#QueryTerminated.queryInfo"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.StreamingQueryListener$QueryStarted"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.StreamingQueryListener$QueryProgress"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.StreamingQueryListener$QueryTerminated"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryListener.onQueryStarted"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryListener.onQueryStarted"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryListener.onQueryProgress"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryListener.onQueryProgress"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryListener.onQueryTerminated"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryListener.onQueryTerminated"),

      // [SPARK-18516][SQL] Split state and progress in streaming
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.SourceStatus"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.SinkStatus"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQuery.sinkStatus"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQuery.sourceStatuses"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.streaming.StreamingQuery.id"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQuery.lastProgress"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQuery.recentProgress"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQuery.id"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryManager.get"),

      // [SPARK-17338][SQL] add global temp view
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.dropGlobalTempView"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.catalog.Catalog.dropTempView"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.dropTempView"),

      // [SPARK-18034] Upgrade to MiMa 0.1.11 to fix flakiness.
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasAggregationDepth.aggregationDepth"),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasAggregationDepth.getAggregationDepth"),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasAggregationDepth.org$apache$spark$ml$param$shared$HasAggregationDepth$_setter_$aggregationDepth_="),

      // [SPARK-18236] Reduce duplicate objects in Spark UI and HistoryServer
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.scheduler.TaskInfo.accumulables"),

      // [SPARK-18657] Add StreamingQuery.runId
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQuery.runId"),

      // [SPARK-18694] Add StreamingQuery.explain and exception to Python and fix StreamingQueryException
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.streaming.StreamingQueryException$"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryException.startOffset"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryException.endOffset"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.streaming.StreamingQueryException.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryException.query")
    )
  }

  // Exclude rules for 2.0.x
  lazy val v20excludes = {
    Seq(
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
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.feature.PCAModel.this"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.status.api.v1.StageData.this"),
      ProblemFilters.exclude[MissingMethodProblem](
        "org.apache.spark.status.api.v1.ApplicationAttemptInfo.this"),
      ProblemFilters.exclude[MissingMethodProblem](
        "org.apache.spark.status.api.v1.ApplicationAttemptInfo.<init>$default$5"),
      // SPARK-14042 Add custom coalescer support
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.rdd.RDD.coalesce"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.rdd.PartitionCoalescer$LocationIterator"),
      ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.rdd.PartitionCoalescer"),
      // SPARK-15532 Remove isRootContext flag from SQLContext.
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLContext.isRootContext"),
      // SPARK-12600 Remove SQL deprecated methods
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLContext$QueryExecution"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLContext$SparkPlanner"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.applySchema"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.parquetFile"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.jdbc"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.jsonFile"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.jsonRDD"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.load"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.dialectClassName"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.getSQLDialect"),
      // SPARK-13664 Replace HadoopFsRelation with FileFormat
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ml.source.libsvm.LibSVMRelation"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.HadoopFsRelationProvider"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.HadoopFsRelation$FileStatusCache"),
      // SPARK-15543 Rename DefaultSources to make them more self-describing
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ml.source.libsvm.DefaultSource")
    ) ++ Seq(
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.SparkContext.emptyRDD"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.broadcast.HttpBroadcastFactory"),
      // SPARK-14358 SparkListener from trait to abstract class
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.SparkContext.addSparkListener"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.JavaSparkListener"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.SparkFirehoseListener"),
      ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.scheduler.SparkListener"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ui.jobs.JobProgressListener"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ui.exec.ExecutorsListener"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ui.env.EnvironmentListener"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ui.storage.StorageListener"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.storage.StorageStatusListener")
    ) ++
    Seq(
      // SPARK-3369 Fix Iterable/Iterator in Java API
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.function.FlatMapFunction.call"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.function.FlatMapFunction.call"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.function.DoubleFlatMapFunction.call"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.function.DoubleFlatMapFunction.call"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.function.FlatMapFunction2.call"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.function.FlatMapFunction2.call"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.function.PairFlatMapFunction.call"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.function.PairFlatMapFunction.call"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.function.CoGroupFunction.call"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.function.CoGroupFunction.call"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.function.MapPartitionsFunction.call"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.function.MapPartitionsFunction.call"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.function.FlatMapGroupsFunction.call"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.function.FlatMapGroupsFunction.call")
    ) ++
    Seq(
      // [SPARK-6429] Implement hashCode and equals together
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.Partition.org$apache$spark$Partition$$super=uals")
    ) ++
    Seq(
      // SPARK-4819 replace Guava Optional
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.JavaSparkContext.getCheckpointDir"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.api.java.JavaSparkContext.getSparkHome"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.getCheckpointFile"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.partitioner"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.getCheckpointFile"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.partitioner")
    ) ++
    Seq(
      // SPARK-12481 Remove Hadoop 1.x
      ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.mapred.SparkHadoopMapRedUtil"),
      // SPARK-12615 Remove deprecated APIs in core
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.<init>$default$6"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.numericRDDToDoubleRDDFunctions"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.intToIntWritable"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.intWritableConverter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.writableWritableConverter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.rddToPairRDDFunctions"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.rddToAsyncRDDActions"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.boolToBoolWritable"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.longToLongWritable"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.doubleWritableConverter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.rddToOrderedRDDFunctions"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.floatWritableConverter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.booleanWritableConverter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.stringToText"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.doubleToDoubleWritable"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.bytesWritableConverter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.rddToSequenceFileRDDFunctions"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.bytesToBytesWritable"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.longWritableConverter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.stringWritableConverter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.floatToFloatWritable"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.rddToPairRDDFunctions$default$4"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.TaskContext.addOnCompleteCallback"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.TaskContext.runningLocally"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.TaskContext.attemptId"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.defaultMinSplits"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.SparkContext.runJob"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.runJob"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.tachyonFolderName"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.initLocalProperties"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.clearJars"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.clearFiles"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.SparkContext.this"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.flatMapWith$default$2"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.toArray"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.mapWith$default$2"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.mapPartitionsWithSplit"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.flatMapWith"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.filterWith"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.foreachWith"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.mapWith"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.mapPartitionsWithSplit$default$2"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.SequenceFileRDDFunctions.this"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.splits"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.toArray"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaSparkContext.defaultMinSplits"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaSparkContext.clearJars"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaSparkContext.clearFiles"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.externalBlockStoreFolderName"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.storage.ExternalBlockStore$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.storage.ExternalBlockManager"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.storage.ExternalBlockStore")
    ) ++ Seq(
      // SPARK-12149 Added new fields to ExecutorSummary
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.status.api.v1.ExecutorSummary.this")
    ) ++
    // SPARK-12665 Remove deprecated and unused classes
    Seq(
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.graphx.GraphKryoRegistrator"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.Vector"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.Vector$Multiplier"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.Vector$")
    ) ++ Seq(
      // SPARK-12591 Register OpenHashMapBasedStateMap for Kryo
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.serializer.KryoInputDataInputBridge"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.serializer.KryoOutputDataOutputBridge")
    ) ++ Seq(
      // SPARK-12510 Refactor ActorReceiver to support Java
      ProblemFilters.exclude[AbstractClassProblem]("org.apache.spark.streaming.receiver.ActorReceiver")
    ) ++ Seq(
      // SPARK-12895 Implement TaskMetrics using accumulators
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.TaskContext.internalMetricsToAccumulators"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.TaskContext.collectInternalAccumulators"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.TaskContext.collectAccumulators")
    ) ++ Seq(
      // SPARK-12896 Send only accumulator updates to driver, not TaskMetrics
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.Accumulable.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.Accumulator.this"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.Accumulator.initialValue")
    ) ++ Seq(
      // SPARK-12692 Scala style: Fix the style violation (Space before "," or ":")
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.SparkSink.org$apache$spark$streaming$flume$sink$Logging$$log_"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.SparkSink.org$apache$spark$streaming$flume$sink$Logging$$log__="),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.SparkAvroCallbackHandler.org$apache$spark$streaming$flume$sink$Logging$$log_"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.SparkAvroCallbackHandler.org$apache$spark$streaming$flume$sink$Logging$$log__="),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.Logging.org$apache$spark$streaming$flume$sink$Logging$$log__="),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.Logging.org$apache$spark$streaming$flume$sink$Logging$$log_"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.Logging.org$apache$spark$streaming$flume$sink$Logging$$_log"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.Logging.org$apache$spark$streaming$flume$sink$Logging$$_log_="),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.TransactionProcessor.org$apache$spark$streaming$flume$sink$Logging$$log_"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.flume.sink.TransactionProcessor.org$apache$spark$streaming$flume$sink$Logging$$log__=")
    ) ++ Seq(
      // SPARK-12689 Migrate DDL parsing to the newly absorbed parser
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.execution.datasources.DDLParser"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.execution.datasources.DDLException"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.ddlParser")
    ) ++ Seq(
      // SPARK-7799 Add "streaming-akka" project
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.zeromq.ZeroMQUtils.createStream"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.streaming.zeromq.ZeroMQUtils.createStream"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.streaming.zeromq.ZeroMQUtils.createStream$default$6"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.zeromq.ZeroMQUtils.createStream$default$5"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.StreamingContext.actorStream$default$4"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.StreamingContext.actorStream$default$3"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.StreamingContext.actorStream"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.api.java.JavaStreamingContext.actorStream"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.streaming.zeromq.ZeroMQReceiver"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.streaming.receiver.ActorReceiver$Supervisor")
    ) ++ Seq(
      // SPARK-12348 Remove deprecated Streaming APIs.
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.streaming.dstream.DStream.foreach"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions$default$4"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.StreamingContext.awaitTermination"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.StreamingContext.networkStream"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.streaming.api.java.JavaStreamingContextFactory"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.api.java.JavaStreamingContext.awaitTermination"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.api.java.JavaStreamingContext.sc"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.api.java.JavaDStreamLike.reduceByWindow"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.api.java.JavaDStreamLike.foreachRDD"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.api.java.JavaDStreamLike.foreach"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.streaming.api.java.JavaStreamingContext.getOrCreate")
    ) ++ Seq(
      // SPARK-12847 Remove StreamingListenerBus and post all Streaming events to the same thread as Spark events
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.AsynchronousListenerBus$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.AsynchronousListenerBus")
    ) ++ Seq(
      // SPARK-11622 Make LibSVMRelation extends HadoopFsRelation and Add LibSVMOutputWriter
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.source.libsvm.DefaultSource"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.ml.source.libsvm.DefaultSource.createRelation")
    ) ++ Seq(
      // SPARK-6363 Make Scala 2.11 the default Scala version
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.cleanup"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.metadataCleaner"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.scheduler.cluster.YarnSchedulerBackend$YarnDriverEndpoint"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.scheduler.cluster.YarnSchedulerBackend$YarnSchedulerEndpoint")
    ) ++ Seq(
      // SPARK-7889
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.deploy.history.HistoryServer.org$apache$spark$deploy$history$HistoryServer$@tachSparkUI"),
      // SPARK-13296
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.UDFRegistration.register"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.UserDefinedPythonFunction$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.UserDefinedPythonFunction"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.UserDefinedFunction"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.UserDefinedFunction$")
    ) ++ Seq(
      // SPARK-12995 Remove deprecated APIs in graphx
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.graphx.lib.SVDPlusPlus.runSVDPlusPlus"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.graphx.Graph.mapReduceTriplets"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.graphx.Graph.mapReduceTriplets$default$3"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.graphx.impl.GraphImpl.mapReduceTriplets")
    ) ++ Seq(
      // SPARK-13426 Remove the support of SIMR
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkMasterRegex.SIMR_REGEX")
    ) ++ Seq(
      // SPARK-13413 Remove SparkContext.metricsSystem/schedulerBackend_ setter
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.metricsSystem"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.schedulerBackend_=")
    ) ++ Seq(
      // SPARK-13220 Deprecate yarn-client and yarn-cluster mode
      ProblemFilters.exclude[MissingMethodProblem](
        "org.apache.spark.SparkContext.org$apache$spark$SparkContext$$createTaskScheduler")
    ) ++ Seq(
      // SPARK-13465 TaskContext.
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.TaskContext.addTaskFailureListener")
    ) ++ Seq (
      // SPARK-7729 Executor which has been killed should also be displayed on Executor Tab
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.status.api.v1.ExecutorSummary.this")
    ) ++ Seq(
      // SPARK-13526 Move SQLContext per-session states to new class
      ProblemFilters.exclude[IncompatibleMethTypeProblem](
        "org.apache.spark.sql.UDFRegistration.this")
    ) ++ Seq(
      // [SPARK-13486][SQL] Move SQLConf into an internal package
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLConf"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLConf$SQLConfEntry"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLConf$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SQLConf$SQLConfEntry$")
    ) ++ Seq(
      //SPARK-11011 UserDefinedType serialization should be strongly typed
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.mllib.linalg.VectorUDT.serialize"),
      // SPARK-12073: backpressure rate controller consumes events preferentially from lagging partitions
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.kafka.KafkaTestUtils.createTopic"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.kafka.DirectKafkaInputDStream.maxMessagesPerPartition")
    ) ++ Seq(
      // [SPARK-13244][SQL] Migrates DataFrame to Dataset
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.tables"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.sql"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.baseRelationToDataFrame"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.table"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrame.apply"),

      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.DataFrame"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.DataFrame$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.LegacyFunctions"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.DataFrameHolder"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.DataFrameHolder$"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLImplicits.localSeqToDataFrameHolder"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLImplicits.stringRddToDataFrameHolder"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLImplicits.rddToDataFrameHolder"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLImplicits.longRddToDataFrameHolder"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLImplicits.intRddToDataFrameHolder"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.GroupedDataset"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Dataset.subtract"),

      // [SPARK-14451][SQL] Move encoder definition into Aggregator interface
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.Aggregator.toColumn"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.expressions.Aggregator.bufferEncoder"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.expressions.Aggregator.outputEncoder"),

      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.evaluation.MultilabelMetrics.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.predictions"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionSummary.predictions")
    ) ++ Seq(
      // [SPARK-13686][MLLIB][STREAMING] Add a constructor parameter `reqParam` to (Streaming)LinearRegressionWithSGD
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.regression.LinearRegressionWithSGD.this")
    ) ++ Seq(
      // SPARK-15250 Remove deprecated json API in DataFrameReader
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.DataFrameReader.json")
    ) ++ Seq(
      // SPARK-13920: MIMA checks should apply to @Experimental and @DeveloperAPI APIs
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.Aggregator.combineCombinersByKey"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.Aggregator.combineValuesByKey"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ComplexFutureAction.run"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ComplexFutureAction.runJob"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ComplexFutureAction.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkEnv.actorSystem"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkEnv.cacheManager"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkEnv.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.getConfigurationFromJobContext"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.getTaskAttemptIDFromTaskAttemptContext"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.deploy.SparkHadoopUtil.newConfiguration"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.bytesReadCallback"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.bytesReadCallback_="),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.canEqual"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.productArity"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.productElement"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.productIterator"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.productPrefix"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.setBytesReadCallback"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.updateBytesRead"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.OutputMetrics.canEqual"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.OutputMetrics.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.OutputMetrics.productArity"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.OutputMetrics.productElement"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.OutputMetrics.productIterator"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.OutputMetrics.productPrefix"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleReadMetrics.decFetchWaitTime"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleReadMetrics.decLocalBlocksFetched"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleReadMetrics.decRecordsRead"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleReadMetrics.decRemoteBlocksFetched"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleReadMetrics.decRemoteBytesRead"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.decShuffleBytesWritten"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.decShuffleRecordsWritten"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.decShuffleWriteTime"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.incShuffleBytesWritten"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.incShuffleRecordsWritten"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.incShuffleWriteTime"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.ShuffleWriteMetrics.setShuffleRecordsWritten"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.PCAModel.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.rdd.RDD.mapPartitionsWithContext"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.AccumulableInfo.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate.taskMetrics"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.scheduler.TaskInfo.attempt"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.ExperimentalMethods.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.callUDF"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.callUdf"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.cumeDist"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.denseRank"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.inputFileName"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.isNaN"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.percentRank"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.rowNumber"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.functions.sparkPartitionId"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.BlockStatus.apply"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.BlockStatus.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.BlockStatus.externalBlockStoreSize"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.BlockStatus.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.offHeapUsed"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatus.offHeapUsedByRdd"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.storage.StorageStatusListener.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.streaming.scheduler.BatchInfo.streamIdToNumRecords"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.storageStatusList"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.storage.StorageListener.storageStatusList"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ExceptionFailure.apply"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ExceptionFailure.copy"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ExceptionFailure.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.executor.InputMetrics.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.executor.OutputMetrics.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Estimator.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Pipeline.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.PipelineModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.PredictionModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.PredictionModel.transformImpl"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Predictor.extractLabeledPoints"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Predictor.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Predictor.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Transformer.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionSummary.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.ClassificationModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.GBTClassifier.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.MultilayerPerceptronClassifier.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.NaiveBayes.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.OneVsRest.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.OneVsRestModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.RandomForestClassifier.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.clustering.KMeans.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.clustering.KMeansModel.computeCost"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.clustering.KMeansModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.clustering.LDAModel.logLikelihood"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.clustering.LDAModel.logPerplexity"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.clustering.LDAModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.evaluation.BinaryClassificationEvaluator.evaluate"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.evaluation.Evaluator.evaluate"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator.evaluate"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.evaluation.RegressionEvaluator.evaluate"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.Binarizer.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.Bucketizer.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.ChiSqSelector.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.ChiSqSelectorModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.CountVectorizer.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.CountVectorizerModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.HashingTF.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.IDF.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.IDFModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.IndexToString.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.Interaction.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.MinMaxScaler.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.MinMaxScalerModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.OneHotEncoder.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.PCA.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.PCAModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.QuantileDiscretizer.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.RFormula.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.RFormulaModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.SQLTransformer.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.StandardScaler.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.StandardScalerModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.StopWordsRemover.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.StringIndexer.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.StringIndexerModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.VectorAssembler.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.VectorIndexer.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.VectorIndexerModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.VectorSlicer.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.Word2Vec.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.Word2VecModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.recommendation.ALS.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.recommendation.ALSModel.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.recommendation.ALSModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.AFTSurvivalRegression.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.AFTSurvivalRegressionModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.GBTRegressor.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.IsotonicRegression.extractWeightedLabeledPoints"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.IsotonicRegression.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.IsotonicRegressionModel.extractWeightedLabeledPoints"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.IsotonicRegressionModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.LinearRegression.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.LinearRegressionSummary.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.LinearRegressionTrainingSummary.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.RandomForestRegressor.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.tuning.CrossValidator.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.tuning.CrossValidatorModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.tuning.TrainValidationSplit.fit"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.tuning.TrainValidationSplitModel.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.evaluation.BinaryClassificationMetrics.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.evaluation.MulticlassMetrics.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.evaluation.RegressionMetrics.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.DataFrameNaFunctions.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.DataFrameStatFunctions.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.DataFrameWriter.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.functions.broadcast"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.functions.callUDF"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.sources.CreatableRelationProvider.createRelation"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.sources.InsertableRelation.insert"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionSummary.fMeasureByThreshold"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionSummary.pr"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionSummary.precisionByThreshold"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionSummary.predictions"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionSummary.recallByThreshold"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.BinaryLogisticRegressionSummary.roc"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.clustering.LDAModel.describeTopics"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.Word2VecModel.findSynonyms"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.Word2VecModel.getVectors"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.recommendation.ALSModel.itemFactors"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.recommendation.ALSModel.userFactors"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.regression.LinearRegressionSummary.predictions"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.regression.LinearRegressionSummary.residuals"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.scheduler.AccumulableInfo.name"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.scheduler.AccumulableInfo.value"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameNaFunctions.drop"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameNaFunctions.fill"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameNaFunctions.replace"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameReader.jdbc"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameReader.json"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameReader.load"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameReader.orc"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameReader.parquet"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameReader.table"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameReader.text"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameStatFunctions.crosstab"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameStatFunctions.freqItems"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.DataFrameStatFunctions.sampleBy"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.createExternalTable"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.emptyDataFrame"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.range"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.functions.udf"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.scheduler.JobLogger"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.streaming.receiver.ActorHelper"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.streaming.receiver.ActorSupervisorStrategy"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.streaming.receiver.ActorSupervisorStrategy$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.streaming.receiver.Statistics"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.streaming.receiver.Statistics$"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.executor.InputMetrics"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.executor.InputMetrics$"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.executor.OutputMetrics"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.executor.OutputMetrics$"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.sql.functions$"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.Estimator.fit"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.Predictor.train"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.Transformer.transform"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.evaluation.Evaluator.evaluate"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.scheduler.SparkListener.onOtherEvent"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.CreatableRelationProvider.createRelation"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.InsertableRelation.insert")
    ) ++ Seq(
      // [SPARK-13926] Automatically use Kryo serializer when shuffling RDDs with simple types
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ShuffleDependency.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ShuffleDependency.serializer"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.serializer.Serializer$")
    ) ++ Seq(
      // SPARK-13927: add row/column iterator to local matrices
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.linalg.Matrix.rowIter"),
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.linalg.Matrix.colIter")
    ) ++ Seq(
      // SPARK-13948: MiMa Check should catch if the visibility change to `private`
      // TODO(josh): Some of these may be legitimate incompatibilities; we should follow up before the 2.0.0 release
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Dataset.toDS"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.sources.OutputWriterFactory.newInstance"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.util.RpcUtils.askTimeout"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.util.RpcUtils.lookupTimeout"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.UnaryTransformer.transform"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.DecisionTreeClassifier.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegression.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.DecisionTreeRegressor.train"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.sql.Dataset.groupBy"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.Dataset.groupBy"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.Dataset.select"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.Dataset.toDF"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.Logging.initializeLogIfNecessary"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.scheduler.SparkListenerEvent.logEvent"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.OutputWriterFactory.newInstance")
    ) ++ Seq(
      // [SPARK-14014] Replace existing analysis.Catalog with SessionCatalog
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLContext.this")
    ) ++ Seq(
      // [SPARK-13928] Move org.apache.spark.Logging into org.apache.spark.internal.Logging
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.Logging"),
      (problem: Problem) => problem match {
        case MissingTypesProblem(_, missing)
          if missing.map(_.fullName).sameElements(Seq("org.apache.spark.Logging")) => false
        case _ => true
      }
    ) ++ Seq(
      // [SPARK-13990] Automatically pick serializer when caching RDDs
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.network.netty.NettyBlockTransferService.uploadBlock")
    ) ++ Seq(
      // [SPARK-14089][CORE][MLLIB] Remove methods that has been deprecated since 1.1, 1.2, 1.3, 1.4, and 1.5
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkEnv.getThreadLocal"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.rdd.RDDFunctions.treeReduce"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.rdd.RDDFunctions.treeAggregate"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.tree.configuration.Strategy.defaultStategy"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.util.MLUtils.loadLibSVMFile"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.util.MLUtils.loadLibSVMFile"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.util.MLUtils.loadLibSVMFile"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.util.MLUtils.saveLabeledData"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.util.MLUtils.loadLabeledData"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.optimization.LBFGS.setMaxNumIterations"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.evaluation.BinaryClassificationEvaluator.setScoreCol")
    ) ++ Seq(
      // [SPARK-14205][SQL] remove trait Queryable
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.sql.Dataset")
    ) ++ Seq(
      // [SPARK-11262][ML] Unit test for gradient, loss layers, memory management
      // for multilayer perceptron.
      // This class is marked as `private`.
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ml.ann.SoftmaxFunction")
    ) ++ Seq(
      // [SPARK-13674][SQL] Add wholestage codegen support to Sample
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.util.random.PoissonSampler.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.util.random.PoissonSampler.this")
    ) ++ Seq(
      // [SPARK-13430][ML] moved featureCol from LinearRegressionModelSummary to LinearRegressionSummary
      ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.ml.regression.LinearRegressionSummary.this")
    ) ++ Seq(
      // [SPARK-14437][Core] Use the address that NettyBlockTransferService listens to create BlockManagerId
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.network.netty.NettyBlockTransferService.this")
    ) ++ Seq(
      // [SPARK-13048][ML][MLLIB] keepLastCheckpoint option for LDA EM optimizer
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.mllib.clustering.DistributedLDAModel.this")
    ) ++ Seq(
      // [SPARK-14475] Propagate user-defined context from driver to executors
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.getLocalProperty"),
      // [SPARK-14617] Remove deprecated APIs in TaskMetrics
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.executor.InputMetrics$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.executor.OutputMetrics$"),
      // [SPARK-14628] Simplify task metrics by always tracking read/write metrics
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.readMethod"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.OutputMetrics.writeMethod")
    ) ++ Seq(
      // SPARK-14628: Always track input/output/shuffle metrics
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.ShuffleReadMetrics.totalBlocksFetched"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.status.api.v1.ShuffleReadMetrics.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.TaskMetrics.inputMetrics"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.TaskMetrics.outputMetrics"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.TaskMetrics.shuffleWriteMetrics"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.TaskMetrics.shuffleReadMetrics"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.status.api.v1.TaskMetrics.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.TaskMetricDistributions.inputMetrics"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.TaskMetricDistributions.outputMetrics"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.TaskMetricDistributions.shuffleWriteMetrics"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.TaskMetricDistributions.shuffleReadMetrics"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.status.api.v1.TaskMetricDistributions.this")
    ) ++ Seq(
      // SPARK-13643: Move functionality from SQLContext to SparkSession
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.SQLContext.getSchema")
    ) ++ Seq(
      // [SPARK-14407] Hides HadoopFsRelation related data source API into execution package
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.OutputWriter"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.OutputWriterFactory")
    ) ++ Seq(
      // SPARK-14734: Add conversions between mllib and ml Vector, Matrix types
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.mllib.linalg.Vector.asML"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.mllib.linalg.Matrix.asML")
    ) ++ Seq(
      // SPARK-14704: Create accumulators in TaskMetrics
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.InputMetrics.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.OutputMetrics.this")
    ) ++ Seq(
      // SPARK-14861: Replace internal usages of SQLContext with SparkSession
      ProblemFilters.exclude[IncompatibleMethTypeProblem](
        "org.apache.spark.ml.clustering.LocalLDAModel.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem](
        "org.apache.spark.ml.clustering.DistributedLDAModel.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem](
        "org.apache.spark.ml.clustering.LDAModel.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "org.apache.spark.ml.clustering.LDAModel.sqlContext"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem](
        "org.apache.spark.sql.Dataset.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem](
        "org.apache.spark.sql.DataFrameReader.this")
    ) ++ Seq(
      // SPARK-14542 configurable buffer size for pipe RDD
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.rdd.RDD.pipe"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.pipe")
    ) ++ Seq(
      // [SPARK-4452][Core]Shuffle data structures can starve others on the same thread for memory
      ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.util.collection.Spillable")
    ) ++ Seq(
      // [SPARK-14952][Core][ML] Remove methods deprecated in 1.6
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.input.PortableDataStream.close"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.weights"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.LinearRegressionModel.weights")
    ) ++ Seq(
      // [SPARK-10653] [Core] Remove unnecessary things from SparkEnv
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkEnv.sparkFilesDir"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.SparkEnv.blockTransferService")
    ) ++ Seq(
      // SPARK-14654: New accumulator API
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ExceptionFailure$"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ExceptionFailure.apply"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ExceptionFailure.metrics"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ExceptionFailure.copy"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ExceptionFailure.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.executor.ShuffleReadMetrics.remoteBlocksFetched"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.executor.ShuffleReadMetrics.totalBlocksFetched"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.executor.ShuffleReadMetrics.localBlocksFetched"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.ShuffleReadMetrics.remoteBlocksFetched"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.status.api.v1.ShuffleReadMetrics.localBlocksFetched")
    ) ++ Seq(
      // [SPARK-14615][ML] Use the new ML Vector and Matrix in the ML pipeline based algorithms
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.clustering.LDAModel.getOldDocConcentration"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.clustering.LDAModel.estimatedDocConcentration"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.clustering.LDAModel.topicsMatrix"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.clustering.KMeansModel.clusterCenters"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LabelConverter.decodeLabel"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LabelConverter.encodeLabeledPoint"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel.weights"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel.predict"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.NaiveBayesModel.predictRaw"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.NaiveBayesModel.raw2probabilityInPlace"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.NaiveBayesModel.theta"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.NaiveBayesModel.pi"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.NaiveBayesModel.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.probability2prediction"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.predictRaw"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.raw2prediction"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.raw2probabilityInPlace"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.predict"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.coefficients"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.ClassificationModel.raw2prediction"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.ClassificationModel.predictRaw"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.classification.ClassificationModel.predictRaw"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.ElementwiseProduct.getScalingVec"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.ElementwiseProduct.setScalingVec"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.PCAModel.pc"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.MinMaxScalerModel.originalMax"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.MinMaxScalerModel.originalMin"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.MinMaxScalerModel.this"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.Word2VecModel.findSynonyms"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.IDFModel.idf"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.StandardScalerModel.mean"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.StandardScalerModel.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.feature.StandardScalerModel.std"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.AFTSurvivalRegressionModel.predict"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.regression.AFTSurvivalRegressionModel.coefficients"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.AFTSurvivalRegressionModel.predictQuantiles"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.AFTSurvivalRegressionModel.this"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.regression.IsotonicRegressionModel.predictions"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.regression.IsotonicRegressionModel.boundaries"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.LinearRegressionModel.predict"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.regression.LinearRegressionModel.coefficients"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.regression.LinearRegressionModel.this")
    ) ++ Seq(
      // [SPARK-15290] Move annotations, like @Since / @DeveloperApi, into spark-tags
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.annotation.package$"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.annotation.package"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.annotation.Private"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.annotation.AlphaComponent"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.annotation.Experimental"),
      ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.annotation.DeveloperApi")
    ) ++ Seq(
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.mllib.linalg.Vector.asBreeze"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.mllib.linalg.Matrix.asBreeze")
    ) ++ Seq(
      // [SPARK-15914] Binary compatibility is broken since consolidation of Dataset and DataFrame
      // in Spark 2.0. However, source level compatibility is still maintained.
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.load"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.jsonRDD"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.jsonFile"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.jdbc"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.parquetFile"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.sql.SQLContext.applySchema")
    ) ++ Seq(
      // SPARK-17096: Improve exception string reported through the StreamingQueryListener
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryListener#QueryTerminated.stackTrace"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryListener#QueryTerminated.this")
    ) ++ Seq(
      // SPARK-17406 limit timeline executor events
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorIdToData"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToTasksActive"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToTasksComplete"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToInputRecords"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToShuffleRead"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToTasksFailed"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToShuffleWrite"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToDuration"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToInputBytes"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToLogUrls"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToOutputBytes"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToOutputRecords"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToTotalCores"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToTasksMax"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ui.exec.ExecutorsListener.executorToJvmGCTime")
    ) ++ Seq(
      // [SPARK-17163] Unify logistic regression interface. Private constructor has new signature.
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.this")
    ) ++ Seq(
      // [SPARK-17498] StringIndexer enhancement for handling unseen labels
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.feature.StringIndexer"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.feature.StringIndexerModel")
    ) ++ Seq(
      // [SPARK-17365][Core] Remove/Kill multiple executors together to reduce RPC call time
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.SparkContext")
    ) ++ Seq(
      // [SPARK-12221] Add CPU time to metrics
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskMetrics.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskMetricDistributions.this")
    ) ++ Seq(
      // [SPARK-18481] ML 2.1 QA: Remove deprecated methods for ML
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.PipelineStage.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.param.JavaParams.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.param.Params.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassificationModel.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegression.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.GBTClassifier.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.numTrees"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.feature.ChiSqSelectorModel.setLabelCol"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.evaluation.Evaluator.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressor.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.GBTRegressionModel.validateParams"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.LinearRegressionSummary.model"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.numTrees"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.classification.RandomForestClassifier"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.classification.GBTClassifier"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.classification.GBTClassificationModel"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.regression.RandomForestRegressor"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.regression.GBTRegressor"),
      ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.ml.regression.GBTRegressionModel"),
      ProblemFilters.exclude[FinalMethodProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.getNumTrees"),
      ProblemFilters.exclude[FinalMethodProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.getNumTrees"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.numTrees"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.classification.RandomForestClassificationModel.setFeatureSubsetStrategy"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.numTrees"),
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.ml.regression.RandomForestRegressionModel.setFeatureSubsetStrategy")
    ) ++ Seq(
      // [SPARK-21680][ML][MLLIB]optimzie Vector coompress
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.mllib.linalg.Vector.toSparseWithSize"),
      ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.ml.linalg.Vector.toSparseWithSize")
    ) ++ Seq(
      // [SPARK-3181][ML]Implement huber loss for LinearRegression.
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasLoss.org$apache$spark$ml$param$shared$HasLoss$_setter_$loss_="),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasLoss.getLoss"),
      ProblemFilters.exclude[InheritedNewAbstractMethodProblem]("org.apache.spark.ml.param.shared.HasLoss.loss")
    )
  }

  def excludes(version: String) = version match {
    case v if v.startsWith("3.0") => v30excludes
    case v if v.startsWith("2.4") => v24excludes
    case v if v.startsWith("2.3") => v23excludes
    case v if v.startsWith("2.2") => v22excludes
    case v if v.startsWith("2.1") => v21excludes
    case v if v.startsWith("2.0") => v20excludes
    case _ => Seq()
  }
}
