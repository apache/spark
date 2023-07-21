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

  // Exclude rules for 4.0.x
  lazy val v40excludes = v35excludes ++ Seq(
  )

  // Exclude rules for 3.5.x from 3.4.0
  lazy val v35excludes = defaultExcludes ++ Seq(
    // [SPARK-43165][SQL] Move canWrite to DataTypeUtils
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.types.DataType.canWrite"),
    // [SPARK-43195][CORE] Remove unnecessary serializable wrapper in HadoopFSUtils
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.HadoopFSUtils$SerializableBlockLocation"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.HadoopFSUtils$SerializableBlockLocation$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.HadoopFSUtils$SerializableFileStatus"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.util.HadoopFSUtils$SerializableFileStatus$"),
    // [SPARK-43792][SQL][PYTHON][CONNECT] Add optional pattern for Catalog.listCatalogs
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listCatalogs"),
    // [SPARK-43881][SQL][PYTHON][CONNECT] Add optional pattern for Catalog.listDatabases
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listDatabases"),
    // [SPARK-43961][SQL][PYTHON][CONNECT] Add optional pattern for Catalog.listTables
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listTables"),
    // [SPARK-43992][SQL][PYTHON][CONNECT] Add optional pattern for Catalog.listFunctions
    ProblemFilters.exclude[ReversedMissingMethodProblem]("org.apache.spark.sql.catalog.Catalog.listFunctions"),
     // [SPARK-43919][SQL] Extract JSON functionality out of Row
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Row.json"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.Row.prettyJson"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.MutableAggregationBuffer.json"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.MutableAggregationBuffer.prettyJson"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.expressions.MutableAggregationBuffer.jsonValue"),
    // [SPARK-43952][CORE][CONNECT][SQL] Add SparkContext APIs for query cancellation by tag
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.status.api.v1.JobData.this"),
    // [SPARK-44205][SQL] Extract Catalyst Code from DecimalType
    ProblemFilters.exclude[DirectMissingMethodProblem]("org.apache.spark.sql.types.DecimalType.unapply")
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

    // SPARK-43169: shaded and generated protobuf code
    ProblemFilters.exclude[Problem]("org.sparkproject.spark_core.protobuf.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.status.protobuf.StoreTypes*"),

    // SPARK-43265: Move Error framework to a common utils module
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.QueryContext"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.SparkException"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.SparkException$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.SparkThrowable"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ErrorInfo$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.ErrorSubInfo$"),

    // SPARK-44104: shaded protobuf code and Apis with parameters relocated
    ProblemFilters.exclude[Problem]("org.sparkproject.spark_protobuf.protobuf.*"),
    ProblemFilters.exclude[Problem]("org.apache.spark.sql.protobuf.utils.SchemaConverters.*"),
    
    // SPARK-44255: Relocate StorageLevel to common/utils
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.storage.StorageLevel"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.storage.StorageLevel$"),

    // SPARK-44475: Relocate DataType and Parser to sql/api
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ArrayType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ArrayType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.BinaryType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.BinaryType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.BooleanType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.BooleanType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ByteType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ByteType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.CalendarIntervalType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.CalendarIntervalType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.CharType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.CharType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DataType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DataType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DateType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DateType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DayTimeIntervalType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DayTimeIntervalType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.Decimal"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.Decimal$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ShortType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ShortType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StringType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StringType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StructField"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StructField$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StructType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.StructType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.TimestampNTZType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.TimestampNTZType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.TimestampType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.TimestampType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.UDTRegistration"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.UDTRegistration$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.VarcharType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.VarcharType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.YearMonthIntervalType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.YearMonthIntervalType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DecimalType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DecimalType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType$DoubleAsIfIntegral"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType$DoubleAsIfIntegral$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.DoubleType$DoubleIsConflicted"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType$FloatAsIfIntegral"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType$FloatAsIfIntegral$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.FloatType$FloatIsConflicted"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.IntegerType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.IntegerType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.LongType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.LongType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.MapType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.MapType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.Metadata"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.Metadata$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.MetadataBuilder"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.NullType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.NullType$"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ObjectType"),
    ProblemFilters.exclude[MissingClassProblem]("org.apache.spark.sql.types.ObjectType$"),

    (problem: Problem) => problem match {
      case MissingClassProblem(cls) => !cls.fullName.startsWith("org.sparkproject.jpmml") &&
          !cls.fullName.startsWith("org.sparkproject.dmg.pmml")
      case _ => true
    }
  )

  def excludes(version: String) = version match {
    case v if v.startsWith("4.0") => v40excludes
    case v if v.startsWith("3.5") => v35excludes
    case _ => Seq()
  }
}
