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

  // Exclude rules for 2.1.x
  lazy val v21excludes = v20excludes

  // Exclude rules for 2.0.x
  lazy val v20excludes = {
    Seq(
      excludePackage("org.apache.spark.rpc"),
      excludePackage("org.spark-project.jetty"),
      excludePackage("org.apache.spark.unused"),
      excludePackage("org.apache.spark.unsafe"),
      excludePackage("org.apache.spark.memory"),
      excludePackage("org.apache.spark.util.collection.unsafe"),
      excludePackage("org.apache.spark.sql.catalyst"),
      excludePackage("org.apache.spark.sql.execution"),
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
    )
  }

  def excludes(version: String) = version match {
    case v if v.startsWith("2.1") => v21excludes
    case v if v.startsWith("2.0") => v20excludes
    case _ => Seq()
  }
}
