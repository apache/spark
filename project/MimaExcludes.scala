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
            // TaskContext was promoted to Abstract class
            ProblemFilters.exclude[AbstractClassProblem](
              "org.apache.spark.TaskContext")

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
