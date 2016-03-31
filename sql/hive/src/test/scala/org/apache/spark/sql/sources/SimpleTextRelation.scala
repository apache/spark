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

package org.apache.spark.sql.sources

import java.text.NumberFormat

import com.google.common.base.Objects
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import org.apache.spark.TaskContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, sources}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, expressions}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A simple example [[HadoopFsRelationProvider]].
 */
class SimpleTextSource extends HadoopFsRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      schema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    new SimpleTextRelation(paths, schema, partitionColumns, parameters)(sqlContext)
  }
}

class AppendingTextOutputFormat(outputFile: Path) extends TextOutputFormat[NullWritable, Text] {
  val numberFormat = NumberFormat.getInstance()

  numberFormat.setMinimumIntegerDigits(5)
  numberFormat.setGroupingUsed(false)

  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
    val split = taskAttemptId.getTaskID.getId
    val name = FileOutputFormat.getOutputName(context)
    new Path(outputFile, s"$name-${numberFormat.format(split)}-$uniqueWriteJobId")
  }
}

class SimpleTextOutputWriter(path: String, context: TaskAttemptContext) extends OutputWriter {
  private val recordWriter: RecordWriter[NullWritable, Text] =
    new AppendingTextOutputFormat(new Path(path)).getRecordWriter(context)

  override def write(row: Row): Unit = {
    val serialized = row.toSeq.map { v =>
      if (v == null) "" else v.toString
    }.mkString(",")
    recordWriter.write(null, new Text(serialized))
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}

/**
 * A simple example [[HadoopFsRelation]], used for testing purposes.  Data are stored as comma
 * separated string lines.  When scanning data, schema must be explicitly provided via data source
 * option `"dataSchema"`.
 */
class SimpleTextRelation(
    override val paths: Array[String],
    val maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    parameters: Map[String, String])(
    @transient val sqlContext: SQLContext)
  extends HadoopFsRelation(parameters) {

  import sqlContext.sparkContext

  override val dataSchema: StructType =
    maybeDataSchema.getOrElse(DataType.fromJson(parameters("dataSchema")).asInstanceOf[StructType])

  override def equals(other: Any): Boolean = other match {
    case that: SimpleTextRelation =>
      this.paths.sameElements(that.paths) &&
        this.maybeDataSchema == that.maybeDataSchema &&
        this.dataSchema == that.dataSchema &&
        this.partitionColumns == that.partitionColumns

    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(paths, maybeDataSchema, dataSchema, partitionColumns)

  override def buildScan(inputStatuses: Array[FileStatus]): RDD[Row] = {
    val fields = dataSchema.map(_.dataType)

    sparkContext.textFile(inputStatuses.map(_.getPath).mkString(",")).map { record =>
      Row(record.split(",", -1).zip(fields).map { case (v, dataType) =>
        val value = if (v == "") null else v
        // `Cast`ed values are always of Catalyst types (i.e. UTF8String instead of String, etc.)
        val catalystValue = Cast(Literal(value), dataType).eval()
        // Here we're converting Catalyst values to Scala values to test `needsConversion`
        CatalystTypeConverters.convertToScala(catalystValue, dataType)
      }: _*)
    }
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus]): RDD[Row] = {

    SimpleTextRelation.requiredColumns = requiredColumns
    SimpleTextRelation.pushedFilters = filters.toSet

    val fields = this.dataSchema.map(_.dataType)
    val inputAttributes = this.dataSchema.toAttributes
    val outputAttributes = requiredColumns.flatMap(name => inputAttributes.find(_.name == name))
    val dataSchema = this.dataSchema

    val inputPaths = inputFiles.map(_.getPath).mkString(",")
    sparkContext.textFile(inputPaths).mapPartitions { iterator =>
      // Constructs a filter predicate to simulate filter push-down
      val predicate = {
        val filterCondition: Expression = filters.collect {
          // According to `unhandledFilters`, `SimpleTextRelation` only handles `GreaterThan` filter
          case sources.GreaterThan(column, value) =>
            val dataType = dataSchema(column).dataType
            val literal = Literal.create(value, dataType)
            val attribute = inputAttributes.find(_.name == column).get
            expressions.GreaterThan(attribute, literal)
        }.reduceOption(expressions.And).getOrElse(Literal(true))
        InterpretedPredicate.create(filterCondition, inputAttributes)
      }

      // Uses a simple projection to simulate column pruning
      val projection = new InterpretedMutableProjection(outputAttributes, inputAttributes)
      val toScala = {
        val requiredSchema = StructType.fromAttributes(outputAttributes)
        CatalystTypeConverters.createToScalaConverter(requiredSchema)
      }

      iterator.map { record =>
        new GenericInternalRow(record.split(",", -1).zip(fields).map {
          case (v, dataType) =>
            val value = if (v == "") null else v
            // `Cast`ed values are always of internal types (e.g. UTF8String instead of String)
            Cast(Literal(value), dataType).eval()
        })
      }.filter { row =>
        predicate(row)
      }.map { row =>
        toScala(projection(row)).asInstanceOf[Row]
      }
    }
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = new OutputWriterFactory {
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
      new SimpleTextOutputWriter(path, context)
    }
  }

  // `SimpleTextRelation` only handles `GreaterThan` filter.  This is used to test filter push-down
  // and `BaseRelation.unhandledFilters()`.
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case _: GreaterThan => false
      case _ => true
    }
  }
}

object SimpleTextRelation {
  // Used to test column pruning
  var requiredColumns: Seq[String] = Nil

  // Used to test filter push-down
  var pushedFilters: Set[Filter] = Set.empty

  // Used to test failed committer
  var failCommitter = false

  // Used to test failed writer
  var failWriter = false

  // Used to test failure callback
  var callbackCalled = false
}

/**
 * A simple example [[HadoopFsRelationProvider]].
 */
class CommitFailureTestSource extends HadoopFsRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      schema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    new CommitFailureTestRelation(paths, schema, partitionColumns, parameters)(sqlContext)
  }
}

class CommitFailureTestRelation(
    override val paths: Array[String],
    maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    parameters: Map[String, String])(
    @transient sqlContext: SQLContext)
  extends SimpleTextRelation(
    paths, maybeDataSchema, userDefinedPartitionColumns, parameters)(sqlContext) {
  override def prepareJobForWrite(job: Job): OutputWriterFactory = new OutputWriterFactory {
    override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
      new SimpleTextOutputWriter(path, context) {
        var failed = false
        TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
          failed = true
          SimpleTextRelation.callbackCalled = true
        }

        override def write(row: Row): Unit = {
          if (SimpleTextRelation.failWriter) {
            sys.error("Intentional task writer failure for testing purpose.")

          }
          super.write(row)
        }

        override def close(): Unit = {
          if (SimpleTextRelation.failCommitter) {
            sys.error("Intentional task commitment failure for testing purpose.")
          }
          super.close()
        }
      }
    }
  }
}
