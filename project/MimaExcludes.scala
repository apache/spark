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
    val excludes =
      SparkBuild.SPARK_VERSION match {
        case v if v.startsWith("1.1") =>
          Seq()
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

