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
 * The Mima build will automatically exclude @DeveloperApi and @Experimental classes. This acts
 * as an official audit of cases where we excluded other classes. Please use the narrowest
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
  def excludes(version: String) = version match {
    case v if v.startsWith("2.0") =>
      Seq(
        excludePackage("org.apache.spark.rpc"),
        excludePackage("org.spark-project.jetty"),
        excludePackage("org.apache.spark.unused"),
        excludePackage("org.apache.spark.unsafe"),
        excludePackage("org.apache.spark.util.collection.unsafe"),
        excludePackage("org.apache.spark.sql.catalyst"),
        excludePackage("org.apache.spark.sql.execution"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.feature.PCAModel.this"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.status.api.v1.StageData.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.status.api.v1.ApplicationAttemptInfo.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.status.api.v1.ApplicationAttemptInfo.<init>$default$5"),
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
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.SQLContext.getSQLDialect")
      ) ++ Seq(
        ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.SparkContext.emptyRDD"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.broadcast.HttpBroadcastFactory")
      ) ++
      Seq(
        // SPARK-3369 Fix Iterable/Iterator in Java API
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.function.FlatMapFunction.call"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.function.FlatMapFunction.call"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.function.DoubleFlatMapFunction.call"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.function.DoubleFlatMapFunction.call"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.function.FlatMapFunction2.call"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.function.FlatMapFunction2.call"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.function.PairFlatMapFunction.call"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.function.PairFlatMapFunction.call"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.function.CoGroupFunction.call"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.function.CoGroupFunction.call"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.function.MapPartitionsFunction.call"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.function.MapPartitionsFunction.call"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.function.FlatMapGroupsFunction.call"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.function.FlatMapGroupsFunction.call")
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
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.Accumulator.this"),
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
      )
    case v if v.startsWith("1.6") =>
      Seq(
        MimaBuild.excludeSparkPackage("deploy"),
        MimaBuild.excludeSparkPackage("network"),
        MimaBuild.excludeSparkPackage("unsafe"),
        // These are needed if checking against the sbt build, since they are part of
        // the maven-generated artifacts in 1.3.
        excludePackage("org.spark-project.jetty"),
        MimaBuild.excludeSparkPackage("unused"),
        // SQL execution is considered private.
        excludePackage("org.apache.spark.sql.execution"),
        // SQL columnar is considered private.
        excludePackage("org.apache.spark.sql.columnar"),
        // The shuffle package is considered private.
        excludePackage("org.apache.spark.shuffle"),
        // The collections utlities are considered pricate.
        excludePackage("org.apache.spark.util.collection")
      ) ++
      MimaBuild.excludeSparkClass("streaming.flume.FlumeTestUtils") ++
      MimaBuild.excludeSparkClass("streaming.flume.PollingFlumeTestUtils") ++
      Seq(
        // MiMa does not deal properly with sealed traits
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.ml.classification.LogisticRegressionSummary.featuresCol")
      ) ++ Seq(
        // SPARK-11530
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.feature.PCAModel.this")
      ) ++ Seq(
        // SPARK-10381 Fix types / units in private AskPermissionToCommitOutput RPC message.
        // This class is marked as `private` but MiMa still seems to be confused by the change.
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.task"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.copy$default$2"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.copy"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.taskAttempt"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.copy$default$3"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.this"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.apply")
      ) ++ Seq(
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.shuffle.FileShuffleBlockResolver$ShuffleFileGroup")
      ) ++ Seq(
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.ml.regression.LeastSquaresAggregator.add"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.ml.regression.LeastSquaresCostFun.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.clearLastInstantiatedContext"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.setLastInstantiatedContext"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.SQLContext$SQLSession"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.detachSession"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.tlSession"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.defaultSession"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.currentSession"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.openSession"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.setSession"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.createSession")
      ) ++ Seq(
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.SparkContext.preferredNodeLocationData_="),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.rdd.MapPartitionsWithPreparationRDD"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.rdd.MapPartitionsWithPreparationRDD$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.SparkSQLParser")
      ) ++ Seq(
        // SPARK-11485
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.DataFrameHolder.df"),
        // SPARK-11541 mark various JDBC dialects as private
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.NoopDialect.productElement"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.NoopDialect.productArity"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.NoopDialect.canEqual"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.NoopDialect.productIterator"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.NoopDialect.productPrefix"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.NoopDialect.toString"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.NoopDialect.hashCode"),
        ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.sql.jdbc.PostgresDialect$"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.PostgresDialect.productElement"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.PostgresDialect.productArity"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.PostgresDialect.canEqual"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.PostgresDialect.productIterator"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.PostgresDialect.productPrefix"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.PostgresDialect.toString"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.sql.jdbc.PostgresDialect.hashCode"),
        ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.sql.jdbc.NoopDialect$")
      ) ++ Seq (
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.status.api.v1.ApplicationInfo.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.status.api.v1.StageData.this")
      ) ++ Seq(
        // SPARK-11766 add toJson to Vector
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Vector.toJson")
      ) ++ Seq(
        // SPARK-9065 Support message handler in Kafka Python API
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper.createDirectStream"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper.createRDD")
      ) ++ Seq(
        // SPARK-4557 Changed foreachRDD to use VoidFunction
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.api.java.JavaDStreamLike.foreachRDD")
      ) ++ Seq(
        // SPARK-11996 Make the executor thread dump work again
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.executor.ExecutorEndpoint"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.executor.ExecutorEndpoint$"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.storage.BlockManagerMessages$GetRpcHostPortForExecutor"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.storage.BlockManagerMessages$GetRpcHostPortForExecutor$")
      ) ++ Seq(
        // SPARK-3580 Add getNumPartitions method to JavaRDD
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.getNumPartitions")
      ) ++ Seq(
        // SPARK-12149 Added new fields to ExecutorSummary
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.status.api.v1.ExecutorSummary.this")
      ) ++
      // SPARK-11314: YARN backend moved to yarn sub-module and MiMA complains even though it's a
      // private class.
      MimaBuild.excludeSparkClass("scheduler.cluster.YarnSchedulerBackend$YarnSchedulerEndpoint")
    case v if v.startsWith("1.5") =>
      Seq(
        MimaBuild.excludeSparkPackage("network"),
        MimaBuild.excludeSparkPackage("deploy"),
        // These are needed if checking against the sbt build, since they are part of
        // the maven-generated artifacts in 1.3.
        excludePackage("org.spark-project.jetty"),
        MimaBuild.excludeSparkPackage("unused"),
        // JavaRDDLike is not meant to be extended by user programs
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.partitioner"),
        // Modification of private static method
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.streaming.kafka.KafkaUtils.org$apache$spark$streaming$kafka$KafkaUtils$$leadersForRanges"),
        // Mima false positive (was a private[spark] class)
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.util.collection.PairIterator"),
        // Removing a testing method from a private class
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.kafka.KafkaTestUtils.waitUntilLeaderOffset"),
        // While private MiMa is still not happy about the changes,
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.ml.regression.LeastSquaresAggregator.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.ml.regression.LeastSquaresCostFun.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.ml.classification.LogisticCostFun.this"),
        // SQL execution is considered private.
        excludePackage("org.apache.spark.sql.execution"),
        // The old JSON RDD is removed in favor of streaming Jackson
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.json.JsonRDD$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.json.JsonRDD"),
        // local function inside a method
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.org$apache$spark$sql$SQLContext$$needsConversion$1"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.UDFRegistration.org$apache$spark$sql$UDFRegistration$$builder$24")
      ) ++ Seq(
        // SPARK-8479 Add numNonzeros and numActives to Matrix.
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrix.numNonzeros"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrix.numActives")
      ) ++ Seq(
        // SPARK-8914 Remove RDDApi
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.RDDApi")
      ) ++ Seq(
        // SPARK-7292 Provide operator to truncate lineage cheaply
        ProblemFilters.exclude[AbstractClassProblem](
          "org.apache.spark.rdd.RDDCheckpointData"),
        ProblemFilters.exclude[AbstractClassProblem](
          "org.apache.spark.rdd.CheckpointRDD")
      ) ++ Seq(
        // SPARK-8701 Add input metadata in the batch page.
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.streaming.scheduler.InputInfo$"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.streaming.scheduler.InputInfo")
      ) ++ Seq(
        // SPARK-6797 Support YARN modes for SparkR
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.r.PairwiseRRDD.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.r.RRDD.createRWorker"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.r.RRDD.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.r.StringRRDD.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.r.BaseRRDD.this")
      ) ++ Seq(
        // SPARK-7422 add argmax for sparse vectors
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Vector.argmax")
      ) ++ Seq(
        // SPARK-8906 Move all internal data source classes into execution.datasources
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.ResolvedDataSource"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PreInsertCastAndRename$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CreateTableUsingAsSelect$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.InsertIntoDataSource$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.SqlNewHadoopPartition"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PartitioningUtils$PartitionValues$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.DefaultWriterContainer"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PartitioningUtils$PartitionValues"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.RefreshTable$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CreateTempTableUsing$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PartitionSpec"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.DynamicPartitionWriterContainer"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CreateTableUsingAsSelect"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.SqlNewHadoopRDD$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.DescribeCommand$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PartitioningUtils$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.SqlNewHadoopRDD"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PreInsertCastAndRename"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.Partition$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.LogicalRelation$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PartitioningUtils"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.LogicalRelation"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.Partition"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.BaseWriterContainer"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PreWriteCheck"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CreateTableUsing"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.RefreshTable"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.SqlNewHadoopRDD$NewHadoopMapPartitionsWithSplitRDD"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.DataSourceStrategy$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CreateTempTableUsing"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CreateTempTableUsingAsSelect$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CreateTempTableUsingAsSelect"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CreateTableUsing$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.ResolvedDataSource$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PreWriteCheck$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.InsertIntoDataSource"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.InsertIntoHadoopFsRelation"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.DDLParser"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.CaseInsensitiveMap"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.InsertIntoHadoopFsRelation$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.DataSourceStrategy"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.SqlNewHadoopRDD$NewHadoopMapPartitionsWithSplitRDD$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.PartitionSpec$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.DescribeCommand"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.sources.DDLException"),
        // SPARK-9763 Minimize exposure of internal SQL classes
        excludePackage("org.apache.spark.sql.parquet"),
        excludePackage("org.apache.spark.sql.json"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCRDD$DecimalConversion$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCPartition"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JdbcUtils$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCRDD$DecimalConversion"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCPartitioningInfo$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCPartition$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.package"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCRDD$JDBCConversion"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCRDD$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.package$DriverWrapper"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCRDD"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCPartitioningInfo"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JdbcUtils"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.DefaultSource"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCRelation$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.package$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.JDBCRelation")
      ) ++ Seq(
        // SPARK-4751 Dynamic allocation for standalone mode
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.SparkContext.supportDynamicAllocation")
      ) ++ Seq(
        // SPARK-9580: Remove SQL test singletons
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.test.LocalSQLContext$SQLSession"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.test.LocalSQLContext"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.test.TestSQLContext"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.test.TestSQLContext$")
      ) ++ Seq(
        // SPARK-9704 Made ProbabilisticClassifier, Identifiable, VectorUDT public APIs
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.mllib.linalg.VectorUDT.serialize")
      ) ++ Seq(
        // SPARK-10381 Fix types / units in private AskPermissionToCommitOutput RPC message.
        // This class is marked as `private` but MiMa still seems to be confused by the change.
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.task"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.copy$default$2"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.copy"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.taskAttempt"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.copy$default$3"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.this"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.scheduler.AskPermissionToCommitOutput.apply")
      )

    case v if v.startsWith("1.4") =>
      Seq(
        MimaBuild.excludeSparkPackage("deploy"),
        MimaBuild.excludeSparkPackage("ml"),
        // SPARK-7910 Adding a method to get the partioner to JavaRDD,
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.partitioner"),
        // SPARK-5922 Adding a generalized diff(other: RDD[(VertexId, VD)]) to VertexRDD
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.graphx.VertexRDD.diff"),
        // These are needed if checking against the sbt build, since they are part of
        // the maven-generated artifacts in 1.3.
        excludePackage("org.spark-project.jetty"),
        MimaBuild.excludeSparkPackage("unused"),
        ProblemFilters.exclude[MissingClassProblem]("com.google.common.base.Optional"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.rdd.JdbcRDD.compute"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.broadcast.HttpBroadcastFactory.newBroadcast"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.broadcast.TorrentBroadcastFactory.newBroadcast"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.scheduler.OutputCommitCoordinator$OutputCommitCoordinatorEndpoint")
      ) ++ Seq(
        // SPARK-4655 - Making Stage an Abstract class broke binary compatility even though
        // the stage class is defined as private[spark]
        ProblemFilters.exclude[AbstractClassProblem]("org.apache.spark.scheduler.Stage")
      ) ++ Seq(
        // SPARK-6510 Add a Graph#minus method acting as Set#difference
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.graphx.VertexRDD.minus")
      ) ++ Seq(
        // SPARK-6492 Fix deadlock in SparkContext.stop()
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.SparkContext.org$" +
            "apache$spark$SparkContext$$SPARK_CONTEXT_CONSTRUCTOR_LOCK")
      )++ Seq(
        // SPARK-6693 add tostring with max lines and width for matrix
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrix.toString")
      )++ Seq(
        // SPARK-6703 Add getOrCreate method to SparkContext
        ProblemFilters.exclude[IncompatibleResultTypeProblem]
            ("org.apache.spark.SparkContext.org$apache$spark$SparkContext$$activeContext")
      )++ Seq(
        // SPARK-7090 Introduce LDAOptimizer to LDA to further improve extensibility
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.mllib.clustering.LDA$EMOptimizer")
      ) ++ Seq(
        // SPARK-6756 add toSparse, toDense, numActives, numNonzeros, and compressed to Vector
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Vector.compressed"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Vector.toDense"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Vector.numNonzeros"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Vector.toSparse"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Vector.numActives"),
        // SPARK-7681 add SparseVector support for gemv
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrix.multiply"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.DenseMatrix.multiply"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.SparseMatrix.multiply")
      ) ++ Seq(
        // Execution should never be included as its always internal.
        MimaBuild.excludeSparkPackage("sql.execution"),
        // This `protected[sql]` method was removed in 1.3.1
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.sql.SQLContext.checkAnalysis"),
        // These `private[sql]` class were removed in 1.4.0:
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.execution.AddExchange"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.execution.AddExchange$"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.PartitionSpec"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.PartitionSpec$"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.Partition"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.Partition$"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.ParquetRelation2$PartitionValues"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.ParquetRelation2$PartitionValues$"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.ParquetRelation2"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.ParquetRelation2$"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.ParquetRelation2$MetadataCache"),
        // These test support classes were moved out of src/main and into src/test:
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.ParquetTestData"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.ParquetTestData$"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.TestGroupWriteSupport"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.CachedData"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.CachedData$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.CacheManager"),
        // TODO: Remove the following rule once ParquetTest has been moved to src/test.
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.sql.parquet.ParquetTest")
      ) ++ Seq(
        // SPARK-7530 Added StreamingContext.getState()
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.StreamingContext.state_=")
      ) ++ Seq(
        // SPARK-7081 changed ShuffleWriter from a trait to an abstract class and removed some
        // unnecessary type bounds in order to fix some compiler warnings that occurred when
        // implementing this interface in Java. Note that ShuffleWriter is private[spark].
        ProblemFilters.exclude[IncompatibleTemplateDefProblem](
          "org.apache.spark.shuffle.ShuffleWriter")
      ) ++ Seq(
        // SPARK-6888 make jdbc driver handling user definable
        // This patch renames some classes to API friendly names.
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.DriverQuirks$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.DriverQuirks"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.PostgresQuirks"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.NoQuirks"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.jdbc.MySQLQuirks")
      )

    case v if v.startsWith("1.3") =>
      Seq(
        MimaBuild.excludeSparkPackage("deploy"),
        MimaBuild.excludeSparkPackage("ml"),
        // These are needed if checking against the sbt build, since they are part of
        // the maven-generated artifacts in the 1.2 build.
        MimaBuild.excludeSparkPackage("unused"),
        ProblemFilters.exclude[MissingClassProblem]("com.google.common.base.Optional")
      ) ++ Seq(
        // SPARK-2321
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.SparkStageInfoImpl.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.SparkStageInfo.submissionTime")
      ) ++ Seq(
        // SPARK-4614
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrices.randn"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrices.rand")
      ) ++ Seq(
        // SPARK-5321
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.SparseMatrix.transposeMultiply"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrix.transpose"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.DenseMatrix.transposeMultiply"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.linalg.Matrix." +
            "org$apache$spark$mllib$linalg$Matrix$_setter_$isTransposed_="),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrix.isTransposed"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.linalg.Matrix.foreachActive")
      ) ++ Seq(
        // SPARK-5540
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.solveLeastSquares"),
        // SPARK-5536
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$^dateFeatures"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$^dateBlock")
      ) ++ Seq(
        // SPARK-3325
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.api.java.JavaDStreamLike.print"),
        // SPARK-2757
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.streaming.flume.sink.SparkAvroCallbackHandler." +
            "removeAndGetProcessor")
      ) ++ Seq(
        // SPARK-5123 (SparkSQL data type change) - alpha component only
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.ml.feature.HashingTF.outputDataType"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.ml.feature.Tokenizer.outputDataType"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.ml.feature.Tokenizer.validateInputType"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.ml.classification.LogisticRegressionModel.validateAndTransformSchema"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.ml.classification.LogisticRegression.validateAndTransformSchema")
      ) ++ Seq(
        // SPARK-4014
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.TaskContext.taskAttemptId"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.TaskContext.attemptNumber")
      ) ++ Seq(
        // SPARK-5166 Spark SQL API stabilization
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Transformer.transform"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Estimator.fit"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.ml.Transformer.transform"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Pipeline.fit"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.PipelineModel.transform"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.ml.Estimator.fit"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.Evaluator.evaluate"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.ml.Evaluator.evaluate"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.tuning.CrossValidator.fit"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.tuning.CrossValidatorModel.transform"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.StandardScaler.fit"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.feature.StandardScalerModel.transform"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegressionModel.transform"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.classification.LogisticRegression.fit"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.ml.evaluation.BinaryClassificationEvaluator.evaluate")
      ) ++ Seq(
        // SPARK-5270
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.isEmpty")
      ) ++ Seq(
        // SPARK-5430
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.treeReduce"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.treeAggregate")
      ) ++ Seq(
        // SPARK-5297 Java FileStream do not work with custom key/values
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.api.java.JavaStreamingContext.fileStream")
      ) ++ Seq(
        // SPARK-5315 Spark Streaming Java API returns Scala DStream
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.api.java.JavaDStreamLike.reduceByWindow")
      ) ++ Seq(
        // SPARK-5461 Graph should have isCheckpointed, getCheckpointFiles methods
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.graphx.Graph.getCheckpointFiles"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.graphx.Graph.isCheckpointed")
      ) ++ Seq(
        // SPARK-4789 Standardize ML Prediction APIs
        ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.mllib.linalg.VectorUDT"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.mllib.linalg.VectorUDT.serialize"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.mllib.linalg.VectorUDT.sqlType")
      ) ++ Seq(
        // SPARK-5814
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$wrapDoubleArray"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$fillFullMatrix"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$iterations"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$makeOutLinkBlock"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$computeYtY"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$makeLinkRDDs"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$alpha"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$randomFactor"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$makeInLinkBlock"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$dspr"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$lambda"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$implicitPrefs"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$rank")
      ) ++ Seq(
        // SPARK-4682
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.RealClock"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.Clock"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.TestClock")
      ) ++ Seq(
        // SPARK-5922 Adding a generalized diff(other: RDD[(VertexId, VD)]) to VertexRDD
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.graphx.VertexRDD.diff")
      )

    case v if v.startsWith("1.2") =>
      Seq(
        MimaBuild.excludeSparkPackage("deploy"),
        MimaBuild.excludeSparkPackage("graphx")
      ) ++
      MimaBuild.excludeSparkClass("mllib.linalg.Matrix") ++
      MimaBuild.excludeSparkClass("mllib.linalg.Vector") ++
      Seq(
        ProblemFilters.exclude[IncompatibleTemplateDefProblem](
          "org.apache.spark.scheduler.TaskLocation"),
        // Added normL1 and normL2 to trait MultivariateStatisticalSummary
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.stat.MultivariateStatisticalSummary.normL1"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.stat.MultivariateStatisticalSummary.normL2"),
        // MapStatus should be private[spark]
        ProblemFilters.exclude[IncompatibleTemplateDefProblem](
          "org.apache.spark.scheduler.MapStatus"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.network.netty.PathResolver"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.spark.network.netty.client.BlockClientListener"),

        // TaskContext was promoted to Abstract class
        ProblemFilters.exclude[AbstractClassProblem](
          "org.apache.spark.TaskContext"),
        ProblemFilters.exclude[IncompatibleTemplateDefProblem](
          "org.apache.spark.util.collection.SortDataFormat")
      ) ++ Seq(
        // Adding new methods to the JavaRDDLike trait:
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.takeAsync"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.foreachPartitionAsync"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.countAsync"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.foreachAsync"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.collectAsync")
      ) ++ Seq(
        // SPARK-3822
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.SparkContext.org$apache$spark$SparkContext$$createTaskScheduler")
      ) ++ Seq(
        // SPARK-1209
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.hadoop.mapreduce.SparkHadoopMapReduceUtil"),
        ProblemFilters.exclude[MissingClassProblem](
          "org.apache.hadoop.mapred.SparkHadoopMapRedUtil"),
        ProblemFilters.exclude[MissingTypesProblem](
          "org.apache.spark.rdd.PairRDDFunctions")
      ) ++ Seq(
        // SPARK-4062
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.kafka.KafkaReceiver#MessageHandler.this")
      )

    case v if v.startsWith("1.1") =>
      Seq(
        MimaBuild.excludeSparkPackage("deploy"),
        MimaBuild.excludeSparkPackage("graphx")
      ) ++
      Seq(
        // Adding new method to JavaRDLike trait - we should probably mark this as a developer API.
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.partitions"),
        // Should probably mark this as Experimental
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.foreachAsync"),
        // We made a mistake earlier (ed06500d3) in the Java API to use default parameter values
        // for countApproxDistinct* functions, which does not work in Java. We later removed
        // them, and use the following to tell Mima to not care about them.
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.JavaPairRDD.countApproxDistinctByKey"),
        ProblemFilters.exclude[IncompatibleResultTypeProblem](
          "org.apache.spark.api.java.JavaPairRDD.countApproxDistinctByKey"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaPairRDD.countApproxDistinct$default$1"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaPairRDD.countApproxDistinctByKey$default$1"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDD.countApproxDistinct$default$1"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaRDDLike.countApproxDistinct$default$1"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.api.java.JavaDoubleRDD.countApproxDistinct$default$1"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.storage.DiskStore.getValues"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.storage.MemoryStore.Entry")
      ) ++
      Seq(
        // Serializer interface change. See SPARK-3045.
        ProblemFilters.exclude[IncompatibleTemplateDefProblem](
          "org.apache.spark.serializer.DeserializationStream"),
        ProblemFilters.exclude[IncompatibleTemplateDefProblem](
          "org.apache.spark.serializer.Serializer"),
        ProblemFilters.exclude[IncompatibleTemplateDefProblem](
          "org.apache.spark.serializer.SerializationStream"),
        ProblemFilters.exclude[IncompatibleTemplateDefProblem](
          "org.apache.spark.serializer.SerializerInstance")
      )++
      Seq(
        // Renamed putValues -> putArray + putIterator
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.storage.MemoryStore.putValues"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.storage.DiskStore.putValues"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.storage.TachyonStore.putValues")
      ) ++
      Seq(
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.streaming.flume.FlumeReceiver.this"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.streaming.kafka.KafkaUtils.createStream"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.streaming.kafka.KafkaReceiver.this")
      ) ++
      Seq( // Ignore some private methods in ALS.
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$^dateFeatures"),
        ProblemFilters.exclude[MissingMethodProblem]( // The only public constructor is the one without arguments.
          "org.apache.spark.mllib.recommendation.ALS.this"),
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$<init>$default$7"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$^dateFeatures")
      ) ++
      MimaBuild.excludeSparkClass("mllib.linalg.distributed.ColumnStatisticsAggregator") ++
      MimaBuild.excludeSparkClass("rdd.ZippedRDD") ++
      MimaBuild.excludeSparkClass("rdd.ZippedPartition") ++
      MimaBuild.excludeSparkClass("util.SerializableHyperLogLog") ++
      MimaBuild.excludeSparkClass("storage.Values") ++
      MimaBuild.excludeSparkClass("storage.Entry") ++
      MimaBuild.excludeSparkClass("storage.MemoryStore$Entry") ++
      // Class was missing "@DeveloperApi" annotation in 1.0.
      MimaBuild.excludeSparkClass("scheduler.SparkListenerApplicationStart") ++
      Seq(
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.mllib.tree.impurity.Gini.calculate"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.mllib.tree.impurity.Entropy.calculate"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem](
          "org.apache.spark.mllib.tree.impurity.Variance.calculate")
      ) ++
      Seq( // Package-private classes removed in SPARK-2341
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.mllib.util.BinaryLabelParser"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.mllib.util.BinaryLabelParser$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.mllib.util.LabelParser"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.mllib.util.LabelParser$"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.mllib.util.MulticlassLabelParser"),
        ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.mllib.util.MulticlassLabelParser$")
      ) ++
      Seq( // package-private classes removed in MLlib
        ProblemFilters.exclude[MissingMethodProblem](
          "org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm.org$apache$spark$mllib$regression$GeneralizedLinearAlgorithm$$prependOne")
      ) ++
      Seq( // new Vector methods in MLlib (binary compatible assuming users do not implement Vector)
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.linalg.Vector.copy")
      ) ++
      Seq( // synthetic methods generated in LabeledPoint
        ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.mllib.regression.LabeledPoint$"),
        ProblemFilters.exclude[IncompatibleMethTypeProblem]("org.apache.spark.mllib.regression.LabeledPoint.apply"),
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.mllib.regression.LabeledPoint.toString")
      ) ++
      Seq ( // Scala 2.11 compatibility fix
        ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.streaming.StreamingContext.<init>$default$2")
      )
    case v if v.startsWith("1.0") =>
      Seq(
        MimaBuild.excludeSparkPackage("api.java"),
        MimaBuild.excludeSparkPackage("mllib"),
        MimaBuild.excludeSparkPackage("streaming")
      ) ++
      MimaBuild.excludeSparkClass("rdd.ClassTags") ++
      MimaBuild.excludeSparkClass("util.XORShiftRandom") ++
      MimaBuild.excludeSparkClass("graphx.EdgeRDD") ++
      MimaBuild.excludeSparkClass("graphx.VertexRDD") ++
      MimaBuild.excludeSparkClass("graphx.impl.GraphImpl") ++
      MimaBuild.excludeSparkClass("graphx.impl.RoutingTable") ++
      MimaBuild.excludeSparkClass("graphx.util.collection.PrimitiveKeyOpenHashMap") ++
      MimaBuild.excludeSparkClass("graphx.util.collection.GraphXPrimitiveKeyOpenHashMap") ++
      MimaBuild.excludeSparkClass("mllib.recommendation.MFDataGenerator") ++
      MimaBuild.excludeSparkClass("mllib.optimization.SquaredGradient") ++
      MimaBuild.excludeSparkClass("mllib.regression.RidgeRegressionWithSGD") ++
      MimaBuild.excludeSparkClass("mllib.regression.LassoWithSGD") ++
      MimaBuild.excludeSparkClass("mllib.regression.LinearRegressionWithSGD")
    case _ => Seq()
  }
}
