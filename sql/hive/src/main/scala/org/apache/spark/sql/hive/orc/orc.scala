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

package org.apache.spark.sql.hive.orc

import java.io.IOException
import java.util.{Locale, Properties}

import scala.collection.JavaConversions._

import org.apache.hadoop.mapred.{JobConf, InputFormat, FileInputFormat}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{SparkContext, SparkHadoopWriter, SerializableWritable, Logging}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive._
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption


/**
 * Allows creation of orc based tables using the syntax
 * `CREATE TEMPORARY TABLE ... USING org.apache.spark.sql.orc`.
 * Currently the only option required is `path`, which should be the location of a collection of,
 * optionally partitioned, orc files.
 */
class DefaultSource
    extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for orc tables."))
  }

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    OrcRelation(checkPath(parameters), parameters, None)(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    OrcRelation(checkPath(parameters), parameters, Some(schema))(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }

    val relation = if (doSave) {
      val createdRelation = createRelation(sqlContext, parameters, data.schema)
      createdRelation.asInstanceOf[OrcRelation].insert(data, true)
      createdRelation
    } else {
      // If the save mode is Ignore, we will just create the relation based on existing data.
      createRelation(sqlContext, parameters)
    }

    relation
  }
}

@DeveloperApi
case class OrcRelation
    (path: String, parameters: Map[String, String], maybeSchema: Option[StructType] = None)
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with CatalystScan
  with InsertableRelation
  with SparkHadoopMapRedUtil
  with HiveInspectors
  with Logging {

  def sparkContext: SparkContext = sqlContext.sparkContext

  // todo: Should calculate per scan size
  override def sizeInBytes: Long = {
    val fs = FileSystem.get(new java.net.URI(path), sparkContext.hadoopConfiguration)
    val fileStatus = fs.getFileStatus(fs.makeQualified(new Path(path)))
    val leaves = SparkHadoopUtil.get.listLeafStatuses(fs, fileStatus.getPath).filter { f =>
      !(f.getPath.getName.startsWith("_") || f.getPath.getName.startsWith("."))
    }
    leaves.map(_.getLen).sum
  }

  private def initialColumnsNamesTypes(schema: StructType) = {
    val inspector = toInspector(schema).asInstanceOf[StructObjectInspector]
    val fields = inspector.getAllStructFieldRefs
    val (columns, columnTypes) = fields.map { f =>
      f.getFieldName -> f.getFieldObjectInspector.getTypeName
    }.unzip
    val columnsNames = columns.mkString(",")
    val columnsTypes = columnTypes.mkString(":")
    (columnsNames, columnsTypes)
  }

  private def orcSchema(
      path: Path,
      configuration: Option[Configuration]): StructType = {
    // get the schema info through ORC Reader
    val conf = configuration.getOrElse(new Configuration())
    val fs: FileSystem = path.getFileSystem(conf)
    val reader = OrcFile.createReader(fs, path)
    require(reader != null, "metadata reader is null!")
    if (reader == null) {
      // return empty seq when saveAsOrcFile
      return StructType(Seq.empty)
    }
    val inspector = reader.getObjectInspector.asInstanceOf[StructObjectInspector]
    // data types that is inspected by this inspector
    val schema = inspector.getTypeName
    HiveMetastoreTypes.toDataType(schema).asInstanceOf[StructType]
  }

  lazy val schema = {
    val fs = FileSystem.get(new java.net.URI(path), sparkContext.hadoopConfiguration)
    val childrenOfPath = fs.listStatus(new Path(path))
      .filterNot(_.getPath.getName.startsWith("_"))
      .filterNot(_.isDir)
    maybeSchema.getOrElse(orcSchema(
      childrenOfPath.head.getPath,
      Some(sparkContext.hadoopConfiguration)))
  }

  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    val sc = sparkContext
    val conf: Configuration = sc.hadoopConfiguration

    val setInputPathsFunc: Option[JobConf => Unit] =
       Some((jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path))

    addColumnIds(output, schema.toAttributes, conf)
    val inputClass =
      classOf[OrcInputFormat].asInstanceOf[Class[_ <: InputFormat[NullWritable, Writable]]]

    // use SpecificMutableRow to decrease GC garbage
    val mutableRow = new SpecificMutableRow(output.map(_.dataType))
    val attrsWithIndex = output.zipWithIndex
    val confBroadcast = sc.broadcast(new SerializableWritable(conf))
    val (columnsNames, columnsTypes) = initialColumnsNamesTypes(schema)
    val rowRdd =
      new HadoopRDD(
        sc,
        confBroadcast,
        setInputPathsFunc,
        inputClass,
        classOf[NullWritable],
        classOf[Writable],
        sc.defaultMinPartitions).mapPartitionsWithInputSplit { (split, iter) =>

        val deserializer = {
          val prop: Properties = new Properties
          prop.setProperty("columns", columnsNames)
          prop.setProperty("columns.types", columnsTypes)

          val serde = new OrcSerde
          serde.initialize(null, prop)
          serde
        }
        HadoopTableReader.fillObject(
          iter.map(_._2),
          deserializer,
          attrsWithIndex,
          mutableRow,
          deserializer)
      }
    rowRdd
  }

  /**
   * add column ids and names
   * @param output
   * @param relationOutput
   * @param conf
   */
  private def addColumnIds(
      output: Seq[Attribute],
      relationOutput: Seq[Attribute],
      conf: Configuration) {
    val names = output.map(_.name)
    val fieldIdMap = relationOutput.map(_.name.toLowerCase(Locale.ENGLISH)).zipWithIndex.toMap
    val ids = output.map { att =>
      val realName = att.name.toLowerCase(Locale.ENGLISH)
      fieldIdMap.getOrElse(realName, -1)
    }.filter(_ >= 0).map(_.asInstanceOf[Integer])

    assert(ids.size == output.size, "columns id and name length does not match!")
    if (ids != null && !ids.isEmpty) {
      HiveShim.appendReadColumns(conf, ids, names)
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // TODO: currently we do not check whether the "schema"s are compatible
    // That means if one first creates a table and then INSERTs data with
    // and incompatible schema the execution will fail. It would be nice
    // to catch this early one, maybe having the planner validate the schema
    // before calling execute().
    import org.apache.hadoop.mapred.{FileOutputFormat, FileOutputCommitter}
    import org.apache.spark.TaskContext

    val (columnsNames, columnsTypes) = initialColumnsNamesTypes(data.schema)
    @transient val job = new JobConf(sqlContext.sparkContext.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Row])
    job.set("mapred.output.format.class", classOf[OrcOutputFormat].getName)
    job.setOutputCommitter(classOf[FileOutputCommitter])
    FileOutputFormat.setOutputPath(job, SparkHadoopWriter.createPathFromString(path, job))

    val conf = new Configuration(job)
    val destinationPath = new Path(path)
    if (overwrite) {
      try {
        destinationPath.getFileSystem(conf).delete(destinationPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${destinationPath.toString} prior" +
              s" to writing to Orc file:\n${e.toString}")
      }
    }

    val taskIdOffset = if (overwrite) {
      1
    } else {
      FileSystemHelper.findMaxTaskId(
        FileOutputFormat.getOutputPath(job).toString, conf) + 1
    }

    val writer = new OrcHadoopWriter(job)
    writer.preSetup()
    sqlContext.sparkContext.runJob(data.queryExecution.executedPlan.execute(), writeShard _)
    writer.commitJob()

    // this function is executed on executor side
    def writeShard(context: TaskContext, iterator: Iterator[Row]): Unit = {
      val nullWritable = NullWritable.get()
      val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt

      val serializer = {
        val prop: Properties = new Properties
        prop.setProperty("columns", columnsNames)
        prop.setProperty("columns.types", columnsTypes)
        val serde = new OrcSerde
        serde.initialize(null, prop)
        serde
      }

      val standardOI = ObjectInspectorUtils
        .getStandardObjectInspector(serializer.getObjectInspector, ObjectInspectorCopyOption.JAVA)
        .asInstanceOf[StructObjectInspector]
      val fieldOIs = standardOI.getAllStructFieldRefs.map(_.getFieldObjectInspector).toArray
      val wrappers = fieldOIs.map(wrapperFor)
      val outputData = new Array[Any](fieldOIs.length)

      writer.setup(context.stageId, context.partitionId + taskIdOffset, taskAttemptId)
      writer.open()
      var row: Row = null
      var i = 0
      try {
        while (iterator.hasNext) {
          row = iterator.next()
          i = 0
          while (i < fieldOIs.length) {
            outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row(i))
            i += 1
          }
          writer.write(nullWritable, serializer.serialize(outputData, standardOI))
        }
      } finally {
        writer.close()
      }
      writer.commit()
    }
  }
}

private[orc] object FileSystemHelper {
  def listFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"OrcTableOperations: Path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (!fs.exists(path) || !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        s"OrcTableOperations: path $path does not exist or is not a directory")
    }
    fs.globStatus(path)
      .flatMap { status => if(status.isDir) fs.listStatus(status.getPath) else List(status) }
      .map(_.getPath)
  }

  /**
   * Finds the maximum taskid in the output file names at the given path.
   */
  def findMaxTaskId(pathStr: String, conf: Configuration): Int = {
    val files = FileSystemHelper.listFiles(pathStr, conf)
    // filename pattern is part-<int>
    val nameP = new scala.util.matching.Regex("""part-(\d{1,})""", "taskid")
    val hiddenFileP = new scala.util.matching.Regex("_.*")
    files.map(_.getName).map {
      case nameP(taskid) => taskid.toInt
      case hiddenFileP() => 0
      case other: String =>
        sys.error("ERROR: attempting to append to set of Orc files and found file" +
          s"that does not match name pattern: $other")
      case _ => 0
    }.reduceLeft((a, b) => if (a < b) b else a)
  }
}

class OrcHadoopWriter(@transient jobConf: JobConf) extends SparkHadoopWriter(jobConf) {
  import java.text.NumberFormat
  import org.apache.hadoop.mapred._

  override def open() {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = "part-"  + numfmt.format(splitID)
    val path = FileOutputFormat.getOutputPath(conf.value)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(conf.value)
      } else {
        FileSystem.get(conf.value)
      }
    }

    // get the path of the temporary output file
    val name = FileOutputFormat.getTaskOutputPath(conf.value, outputName).toString;

    getOutputCommitter().setupTask(getTaskContext())
    writer = getOutputFormat().getRecordWriter(fs, conf.value, name, Reporter.NULL)
  }
}
