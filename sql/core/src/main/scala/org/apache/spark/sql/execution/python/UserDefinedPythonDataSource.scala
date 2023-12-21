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

package org.apache.spark.sql.execution.python

import java.io.{DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import net.razorvine.pickle.Pickler

import org.apache.spark.JobArtifactSet
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, PythonFunction, PythonWorkerUtils, SimplePythonFunction, SpecialLengths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * Data Source V2 wrapper for Python Data Source.
 */
class PythonTableProvider extends TableProvider {
  private var name: String = _
  def setShortName(str: String): Unit = {
    assert(name == null)
    name = str
  }
  private def shortName: String = {
    assert(name != null)
    name
  }
  private var dataSourceInPython: PythonDataSourceCreationResult = _
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
  private lazy val source: UserDefinedPythonDataSource =
    SparkSession.active.sessionState.dataSourceManager.lookupDataSource(shortName)
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (dataSourceInPython == null) {
      dataSourceInPython = source.createDataSourceInPython(shortName, options, None)
    }
    dataSourceInPython.schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val outputSchema = schema
    new Table with SupportsRead {
      override def name(): String = shortName

      override def capabilities(): java.util.Set[TableCapability] = java.util.EnumSet.of(
        BATCH_READ)

      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new ScanBuilder with Batch with Scan {

          private lazy val infoInPython: PythonDataSourceReadInfo = {
            if (dataSourceInPython == null) {
              dataSourceInPython = source
                .createDataSourceInPython(shortName, options, Some(outputSchema))
            }
            source.createReadInfoInPython(dataSourceInPython, outputSchema)
          }

          override def build(): Scan = this

          override def toBatch: Batch = this

          override def readSchema(): StructType = outputSchema

          override def planInputPartitions(): Array[InputPartition] =
            infoInPython.partitions.zipWithIndex.map(p => PythonInputPartition(p._2, p._1)).toArray

          override def createReaderFactory(): PartitionReaderFactory = {
            val readerFunc = infoInPython.func
            new PythonPartitionReaderFactory(
              source, readerFunc, outputSchema, jobArtifactUUID)
          }
          override def description: String = "(Python)"

          override def supportedCustomMetrics(): Array[CustomMetric] =
            source.createPythonMetrics()
        }
      }

      override def schema(): StructType = outputSchema
    }
  }

  override def supportsExternalMetadata(): Boolean = true
}

case class PythonInputPartition(index: Int, pickedPartition: Array[Byte]) extends InputPartition

class PythonPartitionReaderFactory(
    source: UserDefinedPythonDataSource,
    pickledReadFunc: Array[Byte],
    outputSchema: StructType,
    jobArtifactUUID: Option[String])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PartitionReader[InternalRow] {
      // Dummy SQLMetrics. The result is manually reported via DSv2 interface
      // via passing the value to `CustomTaskMetric`. Note that `pythonOtherMetricsDesc`
      // is not used when it is reported. It is to reuse existing Python runner.
      // See also `UserDefinedPythonDataSource.createPythonMetrics`.
      private[this] val metrics: Map[String, SQLMetric] = {
        PythonSQLMetrics.pythonSizeMetricsDesc.keys
          .map(_ -> new SQLMetric("size", -1)).toMap ++
        PythonSQLMetrics.pythonOtherMetricsDesc.keys
          .map(_ -> new SQLMetric("sum", -1)).toMap
      }

      private val outputIter = source.createPartitionReadIteratorInPython(
        partition.asInstanceOf[PythonInputPartition],
        pickledReadFunc,
        outputSchema,
        metrics,
        jobArtifactUUID)

      override def next(): Boolean = outputIter.hasNext

      override def get(): InternalRow = outputIter.next()

      override def close(): Unit = {}

      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        source.createPythonTaskMetrics(metrics.map { case (k, v) => k -> v.value})
      }
    }
  }
}

class PythonCustomMetric(
    override val name: String,
    override val description: String) extends CustomMetric {
  // To allow the aggregation can be called. See `SQLAppStatusListener.aggregateMetrics`
  def this() = this(null, null)

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    SQLMetrics.stringValue("size", taskMetrics, Array.empty[Long])
  }
}

class PythonCustomTaskMetric(
    override val name: String,
    override val value: Long) extends CustomTaskMetric

/**
 * A user-defined Python data source. This is used by the Python API.
 * Defines the interation between Python and JVM.
 *
 * @param dataSourceCls The Python data source class.
 */
case class UserDefinedPythonDataSource(dataSourceCls: PythonFunction) {

  private val inputSchema: StructType = new StructType().add("partition", BinaryType)

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
   * (Driver-side) Run Python process, and get the partition read functions, and
   * partition information.
   */
  def createReadInfoInPython(
      pythonResult: PythonDataSourceCreationResult,
      outputSchema: StructType): PythonDataSourceReadInfo = {
    new UserDefinedPythonDataSourceReadRunner(
      createPythonFunction(
        pythonResult.dataSource), inputSchema, outputSchema).runInPython()
  }

  /**
   * (Executor-side) Create an iterator that reads the input partitions.
   */
  def createPartitionReadIteratorInPython(
      partition: PythonInputPartition,
      pickledReadFunc: Array[Byte],
      outputSchema: StructType,
      metrics: Map[String, SQLMetric],
      jobArtifactUUID: Option[String]): Iterator[InternalRow] = {
    val readerFunc = createPythonFunction(pickledReadFunc)

    val pythonEvalType = PythonEvalType.SQL_MAP_ARROW_ITER_UDF

    val pythonUDF = PythonUDF(
      name = "read_from_data_source",
      func = readerFunc,
      dataType = outputSchema,
      children = toAttributes(inputSchema),
      evalType = pythonEvalType,
      udfDeterministic = false)

    val conf = SQLConf.get

    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
    val evaluatorFactory = new MapInBatchEvaluatorFactory(
      toAttributes(outputSchema),
      Seq(ChainedPythonFunctions(Seq(pythonUDF.func))),
      inputSchema,
      conf.arrowMaxRecordsPerBatch,
      pythonEvalType,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      pythonRunnerConf,
      metrics,
      jobArtifactUUID)

    val part = partition
    evaluatorFactory.createEvaluator().eval(
      part.index, Iterator.single(InternalRow(part.pickedPartition)))
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

  private def createPythonFunction(pickledFunc: Array[Byte]): PythonFunction = {
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

/**
 * Used to store the result of creating a Python data source in the Python process.
 */
case class PythonDataSourceCreationResult(
    dataSource: Array[Byte],
    schema: StructType)

/**
 * A runner used to create a Python data source in a Python process and return the result.
 */
class UserDefinedPythonDataSourceRunner(
    dataSourceCls: PythonFunction,
    provider: String,
    userSpecifiedSchema: Option[StructType],
    options: CaseInsensitiveMap[String])
  extends PythonPlannerRunner[PythonDataSourceCreationResult](dataSourceCls) {

  override val workerModule = "pyspark.sql.worker.create_data_source"

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
      throw QueryCompilationErrors.failToPlanDataSourceError(
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

case class PythonDataSourceReadInfo(
    func: Array[Byte],
    partitions: Seq[Array[Byte]])

/**
 * Send information to a Python process to plan a Python data source read.
 *
 * @param func an Python data source instance
 * @param inputSchema input schema to the data source read from its child plan
 * @param outputSchema output schema of the Python data source
 */
class UserDefinedPythonDataSourceReadRunner(
    func: PythonFunction,
    inputSchema: StructType,
    outputSchema: StructType) extends PythonPlannerRunner[PythonDataSourceReadInfo](func) {

  // See the logic in `pyspark.sql.worker.plan_data_source_read.py`.
  override val workerModule = "pyspark.sql.worker.plan_data_source_read"

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send Python data source
    PythonWorkerUtils.writePythonFunction(func, dataOut)

    // Send input schema
    PythonWorkerUtils.writeUTF(inputSchema.json, dataOut)

    // Send output schema
    PythonWorkerUtils.writeUTF(outputSchema.json, dataOut)

    // Send configurations
    dataOut.writeInt(SQLConf.get.arrowMaxRecordsPerBatch)
  }

  override protected def receiveFromPython(dataIn: DataInputStream): PythonDataSourceReadInfo = {
    // Receive the picked reader or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.failToPlanDataSourceError(
        action = "plan", tpe = "read", msg = msg)
    }

    // Receive the pickled 'read' function.
    val pickledFunction: Array[Byte] = PythonWorkerUtils.readBytes(length, dataIn)

    // Receive the list of partitions, if any.
    val pickledPartitions = ArrayBuffer.empty[Array[Byte]]
    val numPartitions = dataIn.readInt()
    for (_ <- 0 until numPartitions) {
      val pickledPartition: Array[Byte] = PythonWorkerUtils.readBytes(dataIn)
      pickledPartitions.append(pickledPartition)
    }

    PythonDataSourceReadInfo(
      func = pickledFunction,
      partitions = pickledPartitions.toSeq)
  }
}
