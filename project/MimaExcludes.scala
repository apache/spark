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
 */
object MimaExcludes {
    def excludes(version: String) =
      version match {
        case v if v.startsWith("1.4") =>
          Seq(
            MimaBuild.excludeSparkPackage("deploy"),
            MimaBuild.excludeSparkPackage("ml"),
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
              "org.apache.spark.scheduler.OutputCommitCoordinator$OutputCommitCoordinatorActor")
          ) ++ Seq(
            // SPARK-4655 - Making Stage an Abstract class broke binary compatility even though
            // the stage class is defined as private[spark]
            ProblemFilters.exclude[AbstractClassProblem]("org.apache.spark.scheduler.Stage")
          ) ++ Seq(
            // SPARK-6510 Add a Graph#minus method acting as Set#difference
            ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.graphx.VertexRDD.minus")
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
