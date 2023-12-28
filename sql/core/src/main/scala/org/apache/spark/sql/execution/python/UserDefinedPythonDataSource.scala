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

import org.apache.spark.{JobArtifactSet, SparkException}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, PythonFunction, PythonUtils, PythonWorkerUtils, SimplePythonFunction, SpecialLengths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, SupportsTruncate, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
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
    new Table with SupportsRead with SupportsWrite {
      override def name(): String = shortName

      override def capabilities(): java.util.Set[TableCapability] = java.util.EnumSet.of(
        BATCH_READ, BATCH_WRITE, TRUNCATE)

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

      override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
        new WriteBuilder with SupportsTruncate {

          private var isTruncate = false

          override def truncate(): WriteBuilder = {
            isTruncate = true
            this
          }

          override def build(): Write = new Write {

            override def toBatch: BatchWrite = new BatchWrite {

              override def createBatchWriterFactory(
                physicalInfo: PhysicalWriteInfo): DataWriterFactory = {

                val writeInfo = source.createWriteInfoInPython(
                  shortName,
                  info.schema(),
                  info.options(),
                  isTruncate)
                PythonBatchWriterFactory(source, writeInfo.func, info.schema(), jobArtifactUUID)
              }

              // TODO(SPARK-45914): Support commit protocol
              override def commit(messages: Array[WriterCommitMessage]): Unit = {}

              // TODO(SPARK-45914): Support commit protocol
              override def abort(messages: Array[WriterCommitMessage]): Unit = {}
            }

            override def description: String = "(Python)"

            override def supportedCustomMetrics(): Array[CustomMetric] =
              source.createPythonMetrics()
          }
        }
      }
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
  extends PartitionReaderFactory with PythonDataSourceSQLMetrics {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PartitionReader[InternalRow] {

      private[this] val metrics: Map[String, SQLMetric] = pythonMetrics

      private val outputIter = {
        val evaluatorFactory = source.createMapInBatchEvaluatorFactory(
          pickledReadFunc,
          "read_from_data_source",
          UserDefinedPythonDataSource.readInputSchema,
          outputSchema,
          metrics,
          jobArtifactUUID)

        val part = partition.asInstanceOf[PythonInputPartition]
        evaluatorFactory.createEvaluator().eval(
          part.index, Iterator.single(InternalRow(part.pickedPartition)))
      }

      override def next(): Boolean = outputIter.hasNext

      override def get(): InternalRow = outputIter.next()

      override def close(): Unit = {}

      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        source.createPythonTaskMetrics(metrics.map { case (k, v) => k -> v.value})
      }
    }
  }
}

case class PythonWriterCommitMessage(pickledMessage: Array[Byte]) extends WriterCommitMessage

private case class PythonBatchWriterFactory(
    source: UserDefinedPythonDataSource,
    pickledWriteFunc: Array[Byte],
    inputSchema: StructType,
    jobArtifactUUID: Option[String]) extends DataWriterFactory with PythonDataSourceSQLMetrics {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new DataWriter[InternalRow] {

      private[this] val metrics: Map[String, SQLMetric] = pythonMetrics

      private var commitMessage: PythonWriterCommitMessage = _

      override def writeAll(records: java.util.Iterator[InternalRow]): Unit = {
        val evaluatorFactory = source.createMapInBatchEvaluatorFactory(
          pickledWriteFunc,
          "write_to_data_source",
          inputSchema,
          UserDefinedPythonDataSource.writeOutputSchema,
          metrics,
          jobArtifactUUID)
        val outputIter = evaluatorFactory.createEvaluator().eval(partitionId, records.asScala)
        outputIter.foreach { row =>
          if (commitMessage == null) {
            commitMessage = PythonWriterCommitMessage(row.getBinary(0))
          } else {
            throw QueryExecutionErrors.invalidWriterCommitMessageError(details = "more than one")
          }
        }
        if (commitMessage == null) {
          throw QueryExecutionErrors.invalidWriterCommitMessageError(details = "zero")
        }
      }

      override def write(record: InternalRow): Unit =
        SparkException.internalError("write method for Python data source should not be called.")

      override def commit(): WriterCommitMessage = {
        commitMessage.asInstanceOf[WriterCommitMessage]
      }

      override def abort(): Unit = {}

      override def close(): Unit = {}

      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        source.createPythonTaskMetrics(metrics.map { case (k, v) => k -> v.value })
      }
    }
  }
}

trait PythonDataSourceSQLMetrics {
  // Dummy SQLMetrics. The result is manually reported via DSv2 interface
  // via passing the value to `CustomTaskMetric`. Note that `pythonOtherMetricsDesc`
  // is not used when it is reported. It is to reuse existing Python runner.
  // See also `UserDefinedPythonDataSource.createPythonMetrics`.
  protected lazy val pythonMetrics: Map[String, SQLMetric] = {
    PythonSQLMetrics.pythonSizeMetricsDesc.keys
      .map(_ -> new SQLMetric("size", -1)).toMap ++
      PythonSQLMetrics.pythonOtherMetricsDesc.keys
        .map(_ -> new SQLMetric("sum", -1)).toMap
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
      createPythonFunction(pythonResult.dataSource),
      UserDefinedPythonDataSource.readInputSchema,
      outputSchema).runInPython()
  }

  /**
   * (Driver-side) Run Python process and get pickled write function.
   */
  def createWriteInfoInPython(
      provider: String,
      inputSchema: StructType,
      options: CaseInsensitiveStringMap,
      overwrite: Boolean): PythonDataSourceWriteInfo = {
    new UserDefinedPythonDataSourceWriteRunner(
      dataSourceCls,
      provider,
      inputSchema,
      options.asCaseSensitiveMap().asScala.toMap,
      overwrite).runInPython()
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
      jobArtifactUUID: Option[String]): MapInBatchEvaluatorFactory = {
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
      Seq(ChainedPythonFunctions(Seq(pythonUDF.func))),
      inputSchema,
      conf.arrowMaxRecordsPerBatch,
      pythonEvalType,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      pythonRunnerConf,
      metrics,
      jobArtifactUUID)
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
class UserDefinedPythonDataSourceLookupRunner(lookupSources: PythonFunction)
    extends PythonPlannerRunner[PythonLookupAllDataSourcesResult](lookupSources) {

  override val workerModule = "pyspark.sql.worker.lookup_data_sources"

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // No input needed.
  }

  override protected def receiveFromPython(
      dataIn: DataInputStream): PythonLookupAllDataSourcesResult = {
    // Receive the pickled data source or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.failToPlanDataSourceError(
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

/**
 * Hold the results of running [[UserDefinedPythonDataSourceWriteRunner]].
 */
case class PythonDataSourceWriteInfo(func: Array[Byte])

/**
 * A runner that creates a Python data source writer instance and returns a Python function
 * to be used to write data into the data source.
 */
class UserDefinedPythonDataSourceWriteRunner(
    dataSourceCls: PythonFunction,
    provider: String,
    inputSchema: StructType,
    options: Map[String, String],
    overwrite: Boolean) extends PythonPlannerRunner[PythonDataSourceWriteInfo](dataSourceCls) {

  override val workerModule: String = "pyspark.sql.worker.write_into_data_source"

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
  }

  override protected def receiveFromPython(
      dataIn: DataInputStream): PythonDataSourceWriteInfo = {

    // Receive the picked UDF or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.failToPlanDataSourceError(
        action = "plan", tpe = "write", msg = msg)
    }

    // Receive the pickled data source.
    val writeUdf: Array[Byte] = PythonWorkerUtils.readBytes(length, dataIn)

    PythonDataSourceWriteInfo(func = writeUdf)
  }
}
