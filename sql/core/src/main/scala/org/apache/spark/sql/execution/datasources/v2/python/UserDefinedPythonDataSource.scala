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

package org.apache.spark.sql.execution.datasources.v2.python

import java.io.{DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.razorvine.pickle.Pickler

import org.apache.spark.api.python._
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.{ArrowPythonRunner, MapInBatchEvaluatorFactory, PythonPlannerRunner, PythonSQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.VariantVal
import org.apache.spark.util.ArrayImplicits._

/**
 * A user-defined Python data source. This is used by the Python API.
 * Defines the interation between Python and JVM.
 *
 * @param dataSourceCls The Python data source class.
 */
case class UserDefinedPythonDataSource(dataSourceCls: PythonFunction) {

  /**
   * (Driver-side) Run Python process, and get the pickled Python Data Source
   * instance and its schema.
   */
  def createDataSourceInPython(
      shortName: String,
      options: CaseInsensitiveStringMap,
      userSpecifiedSchema: Option[StructType]): PythonDataSourceCreationResult = {
    new UserDefinedPythonDataSourceRunner(
      dataSourceCls,
      shortName,
      userSpecifiedSchema,
      CaseInsensitiveMap(options.asCaseSensitiveMap().asScala.toMap)).runInPython()
  }

  /**
   * (Driver-side) Run Python process to push down filters, get the updated
   * data source instance and the filter pushdown result.
   */
  def pushdownFiltersInPython(
      pythonResult: PythonDataSourceCreationResult,
      outputSchema: StructType,
      filters: Array[Filter]): Option[PythonFilterPushdownResult] = {
    val runner = new UserDefinedPythonDataSourceFilterPushdownRunner(
      createPythonFunction(pythonResult.dataSource),
      outputSchema,
      filters,
      limit = None,
      // Defer planning while limit pushdown is enabled: a later limit pass, or build(), plans
      // once it is known whether a limit is pushed. Plan here only when limit pushdown is off.
      planReadInfo = !SQLConf.get.pythonLimitPushDown
    )
    if (runner.isAnyFilterSupported) {
      Some(runner.runInPython())
    } else {
      None
    }
  }

  /**
   * (Driver-side) Run Python process to plan the read while optionally pushing down a limit, and
   * return a [[PythonFilterPushdownResult]] carrying which filters were pushed, whether the limit
   * was pushed, and the planned read info. The worker plans the read function and partitions in
   * this pass, except when a pushed `limit` is rejected: the reader may have mutated itself while
   * rejecting it, so the result's `readInfo` is then `None` and the caller plans the filters-only
   * read from a fresh reader instead.
   *
   * `limit` is the limit to push, or `None` to plan the read from the filters only -- the
   * build-time path uses `None` when the filter-pushdown pass deferred planning and no limit was
   * ultimately pushed.
   *
   * `filters` must be the filters that were previously pushed via [[pushdownFiltersInPython]] --
   * empty for a limit-only scan, where no filter pushdown ran -- so that replaying them brings
   * the reader to the same state it had during filter pushdown before `pushLimit` is called.
   */
  def pushdownLimitInPython(
      pythonResult: PythonDataSourceCreationResult,
      outputSchema: StructType,
      filters: Array[Filter],
      limit: Option[Int]): PythonFilterPushdownResult = {
    new UserDefinedPythonDataSourceFilterPushdownRunner(
      createPythonFunction(pythonResult.dataSource),
      outputSchema,
      filters,
      limit = limit,
      planReadInfo = true
    ).runInPython()
  }

  /**
   * (Driver-side) Run Python process, and get the partition read functions, and
   * partition information.
   */
  def createReadInfoInPython(
      pythonResult: PythonDataSourceCreationResult,
      outputSchema: StructType,
      isStreaming: Boolean): PythonDataSourceReadInfo = {
    new UserDefinedPythonDataSourceReadRunner(
      createPythonFunction(pythonResult.dataSource),
      UserDefinedPythonDataSource.readInputSchema,
      outputSchema,
      isStreaming).runInPython()
  }

  /**
   * (Driver-side) Run Python process and get pickled write function.
   */
  def createWriteInfoInPython(
      provider: String,
      inputSchema: StructType,
      options: CaseInsensitiveStringMap,
      overwrite: Boolean,
      isStreaming: Boolean): PythonDataSourceWriteInfo = {
    new UserDefinedPythonDataSourceWriteRunner(
      dataSourceCls,
      provider,
      inputSchema,
      options.asCaseSensitiveMap().asScala.toMap,
      overwrite,
      isStreaming).runInPython()
  }

  /**
   * (Driver-side) Run Python process to either commit or abort a write operation.
   */
  def commitWriteInPython(
      writer: Array[Byte],
      messages: Array[WriterCommitMessage],
      abort: Boolean = false): Unit = {
    new UserDefinedPythonDataSourceCommitRunner(
      dataSourceCls, writer, messages, abort).runInPython()
  }

  /**
   * (Executor-side) Create an iterator that execute the Python function.
   */
  def createMapInBatchEvaluatorFactory(
      pickledFunc: Array[Byte],
      funcName: String,
      inputSchema: StructType,
      outputSchema: StructType,
      metrics: Map[String, SQLMetric],
      jobArtifactUUID: Option[String],
      sessionUUID: Option[String]): MapInBatchEvaluatorFactory = {
    val pythonFunc = createPythonFunction(pickledFunc)

    val pythonEvalType = PythonEvalType.SQL_MAP_ARROW_ITER_UDF

    val pythonUDF = PythonUDF(
      name = funcName,
      func = pythonFunc,
      dataType = outputSchema,
      children = toAttributes(inputSchema),
      evalType = pythonEvalType,
      udfDeterministic = false)

    val conf = SQLConf.get

    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
    new MapInBatchEvaluatorFactory(
      toAttributes(outputSchema),
      Seq((ChainedPythonFunctions(Seq(pythonUDF.func)), pythonUDF.resultId.id)),
      inputSchema,
      outputSchema,
      conf.arrowMaxRecordsPerBatch,
      pythonEvalType,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      pythonRunnerConf,
      metrics,
      jobArtifactUUID,
      sessionUUID)
  }

  def createPythonMetrics(): Array[CustomMetric] = {
    // Do not add other metrics such as number of rows,
    // that is already included via DSv2.
    PythonSQLMetrics.pythonSizeMetricsDesc
      .map { case (k, v) => new PythonCustomMetric(k, v)}.toArray
  }

  def createPythonTaskMetrics(taskMetrics: Map[String, Long]): Array[CustomTaskMetric] = {
    taskMetrics.map { case (k, v) => new PythonCustomTaskMetric(k, v)}.toArray
  }

  def createPythonFunction(pickledFunc: Array[Byte]): PythonFunction = {
    SimplePythonFunction(
      command = pickledFunc.toImmutableArraySeq,
      envVars = dataSourceCls.envVars,
      pythonIncludes = dataSourceCls.pythonIncludes,
      pythonExec = dataSourceCls.pythonExec,
      pythonVer = dataSourceCls.pythonVer,
      broadcastVars = dataSourceCls.broadcastVars,
      accumulator = dataSourceCls.accumulator)
  }
}

object UserDefinedPythonDataSource {
  /**
   * The schema of the input to the Python data source read function.
   */
  val readInputSchema: StructType = new StructType().add("partition", BinaryType)

  /**
   * The schema of the output to the Python data source write function.
   */
  val writeOutputSchema: StructType = new StructType().add("message", BinaryType)

  /**
   * (Driver-side) Look up all available Python Data Sources.
   */
  def lookupAllDataSourcesInPython(): PythonLookupAllDataSourcesResult = {
    new UserDefinedPythonDataSourceLookupRunner(
      PythonUtils.createPythonFunction(Array.empty[Byte])).runInPython()
  }
}

/**
 * All Data Sources in Python
 */
case class PythonLookupAllDataSourcesResult(
    names: Array[String], dataSources: Array[Array[Byte]])

/**
 * A runner used to look up Python Data Sources available in Python path.
 */
private class UserDefinedPythonDataSourceLookupRunner(lookupSources: PythonFunction)
    extends PythonPlannerRunner[PythonLookupAllDataSourcesResult](lookupSources) {

  override val workerModule = "pyspark.sql.worker.lookup_data_sources"

  override protected def runnerConf: Map[String, String] = {
    super.runnerConf ++ SQLConf.get.pythonDataSourceProfiler.map(p =>
      Map(SQLConf.PYTHON_DATA_SOURCE_PROFILER.key -> p)
    ).getOrElse(Map.empty)
  }

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // No input needed.
  }

  override protected def receiveFromPython(
      dataIn: DataInputStream): PythonLookupAllDataSourcesResult = {
    // Receive the pickled data source or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "lookup", tpe = "instance", msg = msg)
    }

    val shortNames = ArrayBuffer.empty[String]
    val pickledDataSources = ArrayBuffer.empty[Array[Byte]]
    val numDataSources = length

    for (_ <- 0 until numDataSources) {
      val shortName = PythonWorkerUtils.readUTF(dataIn)
      val pickledDataSource: Array[Byte] = PythonWorkerUtils.readBytes(dataIn)
      shortNames.append(shortName)
      pickledDataSources.append(pickledDataSource)
    }

    PythonLookupAllDataSourcesResult(
      names = shortNames.toArray,
      dataSources = pickledDataSources.toArray)
  }
}

/**
 * Used to store the result of creating a Python data source in the Python process.
 */
case class PythonDataSourceCreationResult(
    dataSource: Array[Byte],
    schema: StructType)

/**
 * A runner used to create a Python data source in a Python process and return the result.
 */
private class UserDefinedPythonDataSourceRunner(
    dataSourceCls: PythonFunction,
    provider: String,
    userSpecifiedSchema: Option[StructType],
    options: CaseInsensitiveMap[String])
  extends PythonPlannerRunner[PythonDataSourceCreationResult](dataSourceCls) {

  override val workerModule = "pyspark.sql.worker.create_data_source"

  override protected def runnerConf: Map[String, String] = {
    super.runnerConf ++ SQLConf.get.pythonDataSourceProfiler.map(p =>
      Map(SQLConf.PYTHON_DATA_SOURCE_PROFILER.key -> p)
    ).getOrElse(Map.empty)
  }

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send python data source
    PythonWorkerUtils.writePythonFunction(dataSourceCls, dataOut)

    // Send the provider name
    PythonWorkerUtils.writeUTF(provider, dataOut)

    // Send the user-specified schema, if provided
    dataOut.writeBoolean(userSpecifiedSchema.isDefined)
    userSpecifiedSchema.map(_.json).foreach(PythonWorkerUtils.writeUTF(_, dataOut))

    // Send the options
    dataOut.writeInt(options.size)
    options.iterator.foreach { case (key, value) =>
      PythonWorkerUtils.writeUTF(key, dataOut)
      PythonWorkerUtils.writeUTF(value, dataOut)
    }
  }

  override protected def receiveFromPython(
      dataIn: DataInputStream): PythonDataSourceCreationResult = {
    // Receive the pickled data source or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "create", tpe = "instance", msg = msg)
    }

    // Receive the pickled data source.
    val pickledDataSourceInstance: Array[Byte] = PythonWorkerUtils.readBytes(length, dataIn)

    // Receive the schema.
    val isDDLString = dataIn.readInt()
    val schemaStr = PythonWorkerUtils.readUTF(dataIn)
    val schema = if (isDDLString == 1) {
      DataType.fromDDL(schemaStr)
    } else {
      DataType.fromJson(schemaStr)
    }
    if (!schema.isInstanceOf[StructType]) {
      throw QueryCompilationErrors.schemaIsNotStructTypeError(schemaStr, schema)
    }

    PythonDataSourceCreationResult(
      dataSource = pickledDataSourceInstance,
      schema = schema.asInstanceOf[StructType])
  }
}

/**
 * @param readInfo The read function and partitions, when the worker planned them. Empty in two
 *                 cases: the filter-pushdown pass deferred planning (limit pushdown enabled), or a
 *                 pushed limit was rejected (the rejecting reader may be mutated). Planning then
 *                 happens on a later limit pass or at build time, from a fresh reader.
 * @param isFilterPushed A sequence of bools indicating whether each filter is pushed down.
 * @param isLimitPushed Whether the limit is pushed down. False when no limit was sent, and also
 *                      when a limit was sent but the reader rejected it.
 */
case class PythonFilterPushdownResult(
    readInfo: Option[PythonDataSourceReadInfo],
    isFilterPushed: collection.Seq[Boolean],
    isLimitPushed: Boolean = false
)

/**
 * Push down filters and optionally a limit to a Python data source.
 *
 * @param dataSource
 *   a Python data source instance
 * @param schema
 *   output schema of the Python data source
 * @param filters
 *   all filters to be pushed down
 * @param limit
 *   the limit to be pushed down, if any
 */
private class UserDefinedPythonDataSourceFilterPushdownRunner(
    dataSource: PythonFunction,
    schema: StructType,
    filters: collection.Seq[Filter],
    limit: Option[Int],
    // Whether the worker should plan (and send back) the read function and partitions in this
    // pass. Kept separate from `limit` so a build-time, filter-only planning pass can request
    // planning without a limit -- deriving it from `limit.isDefined` would force such a pass to
    // smuggle a dummy limit value in just to trigger planning.
    planReadInfo: Boolean)
    extends PythonPlannerRunner[PythonFilterPushdownResult](dataSource) {

  private case class SerializedFilter(
      name: String,
      columnPath: collection.Seq[String],
      @JsonInclude(JsonInclude.Include.NON_ABSENT)
      value: Option[VariantVal],
      @JsonInclude(JsonInclude.Include.NON_DEFAULT)
      isNegated: Boolean,
      @JsonIgnore
      index: Int
  )

  private val mapper = new ObjectMapper().registerModules(DefaultScalaModule)

  private def getField(attribute: String): (Seq[String], StructField) = {
    val columnPath = CatalystSqlParser.parseMultipartIdentifier(attribute)
    val (_, field) = schema
      .findNestedField(columnPath, includeCollections = true)
      .getOrElse(
        throw QueryCompilationErrors.pythonDataSourceError(
          action = "plan",
          tpe = "filter",
          msg = s"Cannot find field $columnPath in schema"
        )
      )
    (columnPath, field)
  }

  private val serializedFilters = filters.zipWithIndex.flatMap {
    case (filter, i) =>
      // Unwrap Not filter
      val (childFilter, isNegated) = filter match {
        case sources.Not(f) => (f, true)
        case _ => (filter, false)
      }

      def construct(
          name: String,
          attribute: String,
          value: Option[Any],
          mapDataType: DataType => DataType = identity): Option[SerializedFilter] = {
        val (columnPath, field) = getField(attribute)
        val dataType = mapDataType(field.dataType)
        val variant = for (v <- value) yield {
          val catalystValue = CatalystTypeConverters.convertToCatalyst(v)
          try {
            VariantExpressionEvalUtils.castToVariant(catalystValue, dataType)
          } catch {
            case _: MatchError =>
              // filter is unsupported if we can't cast it to variant
              return None
          }
        }
        Some(SerializedFilter(name, columnPath, variant, isNegated, i))
      }

      childFilter match {
        case sources.EqualTo(attribute, value) =>
          construct("EqualTo", attribute, Some(value))
        case sources.EqualNullSafe(attribute, value) =>
          construct("EqualNullSafe", attribute, Some(value))
        case sources.GreaterThan(attribute, value) =>
          construct("GreaterThan", attribute, Some(value))
        case sources.GreaterThanOrEqual(attribute, value) =>
          construct("GreaterThanOrEqual", attribute, Some(value))
        case sources.LessThan(attribute, value) =>
          construct("LessThan", attribute, Some(value))
        case sources.LessThanOrEqual(attribute, value) =>
          construct("LessThanOrEqual", attribute, Some(value))
        case sources.In(attribute, value) =>
          construct("In", attribute, Some(value), ArrayType(_))
        case sources.IsNull(attribute) =>
          construct("IsNull", attribute, None)
        case sources.IsNotNull(attribute) =>
          construct("IsNotNull", attribute, None)
        case sources.StringStartsWith(attribute, value) =>
          construct("StringStartsWith", attribute, Some(value))
        case sources.StringEndsWith(attribute, value) =>
          construct("StringEndsWith", attribute, Some(value))
        case sources.StringContains(attribute, value) =>
          construct("StringContains", attribute, Some(value))
        // collation aware filters are currently not supported
        // And, Or are currently not supported
        case _ =>
          None
      }
  }

  // See the logic in `pyspark.sql.worker.data_source_pushdown_filters.py`.
  override val workerModule = "pyspark.sql.worker.data_source_pushdown_filters"

  override protected def runnerConf: Map[String, String] = {
    super.runnerConf ++ SQLConf.get.pythonDataSourceProfiler.map(p =>
      Map(SQLConf.PYTHON_DATA_SOURCE_PROFILER.key -> p)
    ).getOrElse(Map.empty)
  }

  def isAnyFilterSupported: Boolean = serializedFilters.nonEmpty

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send Python data source
    PythonWorkerUtils.writePythonFunction(dataSource, dataOut)

    // Send output schema
    PythonWorkerUtils.writeUTF(schema.json, dataOut)

    // Send the filters
    PythonWorkerUtils.writeUTF(mapper.writeValueAsString(serializedFilters), dataOut)

    // Send the limit, if any. -1 means no limit is pushed down.
    dataOut.writeInt(limit.getOrElse(-1))

    // Send configurations
    dataOut.writeInt(SQLConf.get.arrowMaxRecordsPerBatch)
    dataOut.writeBoolean(SQLConf.get.pythonFilterPushDown)
    dataOut.writeBoolean(SQLConf.get.pythonLimitPushDown)
    dataOut.writeBoolean(SQLConf.get.pysparkBinaryAsBytes)
    // Whether the worker should plan the read function and partitions. The filter-pushdown pass
    // sets this false while limit pushdown is enabled, because a later limit pass -- or
    // build-time planning -- will plan once it is known whether a limit is pushed, so
    // `partitions()`/`read()` never run on a reader whose plan would be discarded.
    dataOut.writeBoolean(planReadInfo)
  }

  override protected def receiveFromPython(dataIn: DataInputStream): PythonFilterPushdownResult = {
    // Whether a read function + partitions follow. Read first so that an exception raised in the
    // worker (sent as PYTHON_EXCEPTION_THROWN) is surfaced here instead of being misread as data.
    val hasReadInfo = dataIn.readInt()
    if (hasReadInfo == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "plan",
        tpe = "read",
        msg = msg
      )
    }
    // Receive the read function and partitions, when the worker planned them. None follow when the
    // filter-pushdown pass defers planning (limit pushdown enabled) or when a pushed limit was
    // rejected; build() (or a later limit pass) then plans them from a fresh reader instead.
    val readInfo = if (hasReadInfo != 0) {
      Some(PythonDataSourceReadInfo.receive(dataIn))
    } else {
      None
    }

    // Receive the pushed filters as a list of indices.
    val numFiltersPushed = dataIn.readInt()
    val isFilterPushed = ArrayBuffer.fill(filters.length)(false)
    for (_ <- 0 until numFiltersPushed) {
      val i = dataIn.readInt()
      isFilterPushed(serializedFilters(i).index) = true
    }

    // Receive whether the limit was pushed down, sent as 1 or 0.
    val isLimitPushed = dataIn.readInt() != 0

    PythonFilterPushdownResult(
      readInfo = readInfo,
      isFilterPushed = isFilterPushed,
      isLimitPushed = isLimitPushed
    )
  }
}

case class PythonDataSourceReadInfo(
    func: Array[Byte],
    partitions: Seq[Array[Byte]])

object PythonDataSourceReadInfo {
  def receive(dataIn: DataInputStream): PythonDataSourceReadInfo = {
    // Receive the picked reader or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "initialize",
        tpe = "reader",
        msg = msg
      )
    }

    // Receive the pickled 'read' function.
    val pickledFunction: Array[Byte] = PythonWorkerUtils.readBytes(length, dataIn)

    // Receive the list of partitions, if any.
    val pickledPartitions = ArrayBuffer.empty[Array[Byte]]
    val numPartitions = dataIn.readInt()
    if (numPartitions == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "generate",
        tpe = "read partitions",
        msg = msg
      )
    }
    for (_ <- 0 until numPartitions) {
      val pickledPartition: Array[Byte] = PythonWorkerUtils.readBytes(dataIn)
      pickledPartitions.append(pickledPartition)
    }

    PythonDataSourceReadInfo(func = pickledFunction, partitions = pickledPartitions.toSeq)
  }
}

/**
 * Send information to a Python process to plan a Python data source read.
 *
 * @param func a Python data source instance
 * @param inputSchema input schema to the data source read from its child plan
 * @param outputSchema output schema of the Python data source
 */
private class UserDefinedPythonDataSourceReadRunner(
    func: PythonFunction,
    inputSchema: StructType,
    outputSchema: StructType,
    isStreaming: Boolean) extends PythonPlannerRunner[PythonDataSourceReadInfo](func) {

  // See the logic in `pyspark.sql.worker.plan_data_source_read.py`.
  override val workerModule = "pyspark.sql.worker.plan_data_source_read"

  override protected def runnerConf: Map[String, String] = {
    super.runnerConf ++ SQLConf.get.pythonDataSourceProfiler.map(p =>
      Map(SQLConf.PYTHON_DATA_SOURCE_PROFILER.key -> p)
    ).getOrElse(Map.empty)
  }

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send Python data source
    PythonWorkerUtils.writePythonFunction(func, dataOut)

    // Send input schema
    PythonWorkerUtils.writeUTF(inputSchema.json, dataOut)

    // Send output schema
    PythonWorkerUtils.writeUTF(outputSchema.json, dataOut)

    // Send configurations
    dataOut.writeInt(SQLConf.get.arrowMaxRecordsPerBatch)
    dataOut.writeBoolean(SQLConf.get.pythonFilterPushDown)
    dataOut.writeBoolean(SQLConf.get.pythonLimitPushDown)

    dataOut.writeBoolean(isStreaming)
    dataOut.writeBoolean(SQLConf.get.pysparkBinaryAsBytes)
  }

  override protected def receiveFromPython(dataIn: DataInputStream): PythonDataSourceReadInfo = {
    PythonDataSourceReadInfo.receive(dataIn)
  }
}

/**
 * Hold the results of running [[UserDefinedPythonDataSourceWriteRunner]].
 */
case class PythonDataSourceWriteInfo(func: Array[Byte], writer: Array[Byte])

/**
 * A runner that creates a Python data source writer instance and returns a Python function
 * to be used to write data into the data source.
 */
private class UserDefinedPythonDataSourceWriteRunner(
    dataSourceCls: PythonFunction,
    provider: String,
    inputSchema: StructType,
    options: Map[String, String],
    overwrite: Boolean,
    isStreaming: Boolean) extends PythonPlannerRunner[PythonDataSourceWriteInfo](dataSourceCls) {

  override val workerModule: String = "pyspark.sql.worker.write_into_data_source"

  override protected def runnerConf: Map[String, String] = {
    super.runnerConf ++ SQLConf.get.pythonDataSourceProfiler.map(p =>
      Map(SQLConf.PYTHON_DATA_SOURCE_PROFILER.key -> p)
    ).getOrElse(Map.empty)
  }

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send the Python data source class.
    PythonWorkerUtils.writePythonFunction(dataSourceCls, dataOut)

    // Send the provider name
    PythonWorkerUtils.writeUTF(provider, dataOut)

    // Send the input schema
    PythonWorkerUtils.writeUTF(inputSchema.json, dataOut)

    // Send the return type
    PythonWorkerUtils.writeUTF(UserDefinedPythonDataSource.writeOutputSchema.json, dataOut)

    // Send the options
    dataOut.writeInt(options.size)
    options.iterator.foreach { case (key, value) =>
      PythonWorkerUtils.writeUTF(key, dataOut)
      PythonWorkerUtils.writeUTF(value, dataOut)
    }

    // Send the `overwrite` flag
    dataOut.writeBoolean(overwrite)

    dataOut.writeBoolean(isStreaming)
    dataOut.writeBoolean(SQLConf.get.pysparkBinaryAsBytes)
  }

  override protected def receiveFromPython(
      dataIn: DataInputStream): PythonDataSourceWriteInfo = {

    // Receive the picked UDF or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "initialize", tpe = "writer", msg = msg)
    }

    // Receive the pickled data source write function.
    val writeUdf: Array[Byte] = PythonWorkerUtils.readBytes(length, dataIn)

    // Receive the pickled instance of the data source writer.
    val writer: Array[Byte] = PythonWorkerUtils.readBytes(dataIn)

    PythonDataSourceWriteInfo(func = writeUdf, writer = writer)
  }
}

/**
 * A runner that takes a Python data source writer and a list of commit messages,
 * and invokes the `commit` or `abort` method of the writer in Python.
 */
private class UserDefinedPythonDataSourceCommitRunner(
    dataSourceCls: PythonFunction,
    writer: Array[Byte],
    messages: Array[WriterCommitMessage],
    abort: Boolean) extends PythonPlannerRunner[Unit](dataSourceCls) {
  override val workerModule: String = "pyspark.sql.worker.commit_data_source_write"

  override protected def runnerConf: Map[String, String] = {
    super.runnerConf ++ SQLConf.get.pythonDataSourceProfiler.map(p =>
      Map(SQLConf.PYTHON_DATA_SOURCE_PROFILER.key -> p)
    ).getOrElse(Map.empty)
  }

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send the Python data source writer.
    PythonWorkerUtils.writeBytes(writer, dataOut)

    // Send the commit messages.
    dataOut.writeInt(messages.length)
    messages.foreach { message =>
      // Commit messages can be null if there are task failures.
      if (message == null) {
        dataOut.writeInt(SpecialLengths.NULL)
      } else {
        PythonWorkerUtils.writeBytes(
          message.asInstanceOf[PythonWriterCommitMessage].pickledMessage, dataOut)
      }
    }

    // Send whether to invoke `abort` instead of `commit`.
    dataOut.writeBoolean(abort)
  }

  override protected def receiveFromPython(dataIn: DataInputStream): Unit = {
    // Receive any exceptions thrown in the Python worker.
    val code = dataIn.readInt()
    if (code == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "commit or abort", tpe = "write", msg = msg)
    }
    assert(code == 0, s"Python commit job should run successfully, but got exit code: $code")
  }
}
