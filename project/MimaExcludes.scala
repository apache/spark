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

  // Exclude rules for 3.4.x
  lazy val v34excludes = v33excludes ++ Seq(
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.recommendation.ALS.checkedCast"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.ml.recommendation.ALSModel.checkedCast")
  )

  // Exclude rules for 3.3.x from 3.2.0
  lazy val v33excludes = v32excludes ++ Seq(
    // [SPARK-35672][CORE][YARN] Pass user classpath entries to executors using config instead of command line
    // The followings are necessary for Scala 2.13.
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.executor.CoarseGrainedExecutorBackend#Arguments.*"),
    ProblemFilters.exclude[IncompatibleResultTypeProblem]("org.apache.spark.executor.CoarseGrainedExecutorBackend#Arguments.*"),
    ProblemFilters.exclude[MissingTypesProblem]("org.apache.spark.executor.CoarseGrainedExecutorBackend$Arguments$"),

    // [SPARK-37391][SQL] JdbcConnectionProvider tells if it modifies security context
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.jdbc.JdbcConnectionProvider.modifiesSecurityContext"),

    // [SPARK-37780][SQL] QueryExecutionListener support SQLConf as constructor parameter
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.util.ExecutionListenerManager.this"),
    // [SPARK-37786][SQL] StreamingQueryListener support use SQLConf.get to get corresponding SessionState's SQLConf
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StreamingQueryManager.this"),
    // [SPARK-38432][SQL] Reactor framework so as JDBC dialect could compile filter by self way
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.sources.Filter.toV2"),

    // [SPARK-37831][CORE] Add task partition id in TaskInfo and Task Metrics
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskData.this"),

    // [SPARK-37600][BUILD] Upgrade to Hadoop 3.3.2
    ProblemFilters.exclude[MissingClassProblem]("org.apache.hadoop.shaded.net.jpountz.lz4.LZ4Compressor"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.hadoop.shaded.net.jpountz.lz4.LZ4Factory"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.hadoop.shaded.net.jpountz.lz4.LZ4SafeDecompressor"),

    // [SPARK-37377][SQL] Initial implementation of Storage-Partitioned Join
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.connector.read.partitioning.ClusteredDistribution"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.connector.read.partitioning.Distribution"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.connector.read.partitioning.Partitioning.*"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.connector.read.partitioning.Partitioning.*")
  )

  // Exclude rules for 3.2.x from 3.1.1
  lazy val v32excludes = Seq(
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

    // [SPARK-34848][CORE] Add duration to TaskMetricDistributions
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.TaskMetricDistributions.this"),

    // [SPARK-34488][CORE] Support task Metrics Distributions and executor Metrics Distributions
    // in the REST API call for a specified stage
    ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.status.api.v1.StageData.this"),

    // [SPARK-36173][CORE] Support getting CPU number in TaskContext
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.cpus"),

    // [SPARK-38679][CORE] Expose the number of partitions in a stage to TaskContext
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.TaskContext.numPartitions"),

    // [SPARK-35896] Include more granular metrics for stateful operators in StreamingQueryProgress
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.StateOperatorProgress.this"),

    (problem: Problem) => problem match {
      case MissingClassProblem(cls) => !cls.fullName.startsWith("org.sparkproject.jpmml") &&
          !cls.fullName.startsWith("org.sparkproject.dmg.pmml")
      case _ => true
    },

    // [SPARK-33808][SQL] DataSource V2: Build logical writes in the optimizer
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.connector.write.V1WriteBuilder"),

    // [SPARK-33955] Add latest offsets to source progress
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.streaming.SourceProgress.this"),

    // [SPARK-34862][SQL] Support nested column in ORC vectorized reader
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getBoolean"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getByte"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getShort"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getInt"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getLong"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getFloat"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getDouble"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getDecimal"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getUTF8String"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getBinary"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getArray"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getMap"),
    ProblemFilters.exclude[DirectAbstractMethodProblem]("org.apache.spark.sql.vectorized.ColumnVector.getChild"),

    // [SPARK-35135][CORE] Turn WritablePartitionedIterator from trait into a default implementation class
    ProblemFilters.exclude[IncompatibleTemplateDefProblem]("org.apache.spark.util.collection.WritablePartitionedIterator"),

    // [SPARK-35757][CORE] Add bitwise AND operation and functionality for intersecting bloom filters
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.util.sketch.BloomFilter.intersectInPlace"),

    // [SPARK-35276][CORE] Calculate checksum for shuffle data and write as checksum file
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.shuffle.sort.io.LocalDiskShuffleMapOutputWriter.commitAllPartitions"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.shuffle.sort.io.LocalDiskSingleSpillMapOutputWriter.transferMapSpillFile"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.shuffle.api.ShuffleMapOutputWriter.commitAllPartitions"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter.transferMapSpillFile"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter.transferMapSpillFile"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.shuffle.api.ShuffleMapOutputWriter.commitAllPartitions")
  )

  def excludes(version: String) = version match {
    case v if v.startsWith("3.4") => v34excludes
    case v if v.startsWith("3.3") => v33excludes
    case v if v.startsWith("3.2") => v32excludes
    case _ => Seq()
  }
}
