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
import java.text.SimpleDateFormat
import java.util.{Locale, Date}
import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, FileOutputCommitter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.{Writable, NullWritable}
import org.apache.hadoop.mapreduce.{TaskID, TaskAttemptContext, Job}
import org.apache.hadoop.hive.ql.io.orc.{OrcSerde, OrcInputFormat, OrcOutputFormat}
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.hadoop.mapred.{SparkHadoopMapRedUtil, Reporter, JobConf}

import org.apache.spark.sql.execution._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.FileSystemHelper
import org.apache.spark.{TaskContext, SerializableWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils._

/**
 * orc table scan operator. Imports the file that backs the given
 * [[org.apache.spark.sql.hive.orc.OrcRelation]] as a ``RDD[Row]``.
 */
case class OrcTableScan(
    output: Seq[Attribute],
    relation: OrcRelation,
    columnPruningPred: Option[Expression])
  extends LeafNode {

  @transient
  lazy val serde: OrcSerde = initSerde

  @transient
  lazy val getFieldValue: Seq[Product => Any] = {
    val inspector = serde.getObjectInspector.asInstanceOf[StructObjectInspector]
    output.map(attr => {
      val ref = inspector.getStructFieldRef(attr.name.toLowerCase(Locale.ENGLISH))
      row: Product => {
        val fieldData = row.productElement(1)
        val data = inspector.getStructFieldData(fieldData, ref)
        unwrapData(data, ref.getFieldObjectInspector)
      }
    })
  }

  private def initSerde(): OrcSerde = {
    val serde = new OrcSerde
    serde.initialize(null, relation.prop)
    serde
  }

  def unwrapData(data: Any, oi: ObjectInspector): Any = oi match {
    case pi: PrimitiveObjectInspector => pi.getPrimitiveJavaObject(data)
    case li: ListObjectInspector =>
      Option(li.getList(data))
        .map(_.map(unwrapData(_, li.getListElementObjectInspector)).toSeq)
        .orNull
    case mi: MapObjectInspector =>
      Option(mi.getMap(data)).map(
        _.map {
          case (k, v) =>
            (unwrapData(k, mi.getMapKeyObjectInspector),
              unwrapData(v, mi.getMapValueObjectInspector))
        }.toMap).orNull
    case si: StructObjectInspector =>
      val allRefs = si.getAllStructFieldRefs
      new GenericRow(
        allRefs.map(r =>
          unwrapData(si.getStructFieldData(data, r), r.getFieldObjectInspector)).toArray)
  }

  override def execute(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val job = new Job(sc.hadoopConfiguration)

    val conf: Configuration = job.getConfiguration
    val fileList = FileSystemHelper.listFiles(relation.path, conf)

    // add all paths in the directory but skip "hidden" ones such
    // as "_SUCCESS"
    for (path <- fileList if !path.getName.startsWith("_")) {
      FileInputFormat.addInputPath(job, path)
    }

    setColumnIds(output, relation, conf)
    val inputClass = classOf[OrcInputFormat].asInstanceOf[
      Class[_ <: org.apache.hadoop.mapred.InputFormat[Void, Row]]]

    val rowRdd = productToRowRdd(sc.hadoopRDD[Void, Row](
      conf.asInstanceOf[JobConf], inputClass, classOf[Void], classOf[Row]))
    rowRdd
  }

  /**
   * @param output
   * @param relation
   * @param conf
   */
  def setColumnIds(output: Seq[Attribute], relation: OrcRelation, conf: Configuration) {
    val idBuff = new StringBuilder()

    output.map(att => {
      val realName = att.name.toLowerCase(Locale.ENGLISH)
      val id = relation.fieldIdCache.getOrElse(realName, null)
      if (null != id) {
        idBuff.append(id)
        idBuff.append(",")
      }
    })
    if (idBuff.length > 0) {
      idBuff.setLength(idBuff.length - 1)
    }
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, idBuff.toString())
  }

  /**
   *
   * @param data
   * @tparam A
   * @return
   */
  def productToRowRdd[A <: Product](data: RDD[A]): RDD[Row] = {
    data.mapPartitions {
      iterator =>
        if (iterator.isEmpty) {
          Iterator.empty
        } else {
          val bufferedIterator = iterator.buffered
          bufferedIterator.map {r =>
            val values = getFieldValue.map(_(r))
            new GenericRow(values.map {
              case n: String if n.toLowerCase == "null" => ""
              case varchar: HiveVarchar => varchar.getValue
              case decimal: HiveDecimal =>
                BigDecimal(decimal.bigDecimalValue)
              case null => ""
              case other => other

            }.toArray)
          }
        }
    }
  }

  /**
   * Applies a (candidate) projection.
   *
   * @param prunedAttributes The list of attributes to be used in the projection.
   * @return Pruned TableScan.
   */
  def pruneColumns(prunedAttributes: Seq[Attribute]): OrcTableScan = {
    OrcTableScan(prunedAttributes, relation, columnPruningPred)
  }
}

/**
 * Operator that acts as a sink for queries on RDDs and can be used to
 * store the output inside a directory of ORC files. This operator
 * is similar to Hive's INSERT INTO TABLE operation in the sense that
 * one can choose to either overwrite or append to a directory. Note
 * that consecutive insertions to the same table must have compatible
 * (source) schemas.
 */
private[sql] case class InsertIntoOrcTable(
    relation: OrcRelation,
    child: SparkPlan,
    overwrite: Boolean = false)
  extends UnaryNode with SparkHadoopMapRedUtil with org.apache.spark.Logging {

  override def output = child.output

  val inputClass = getInputClass.getName

  @transient val sc = sqlContext.sparkContext

  @transient lazy val orcSerde = initFieldInfo

  private def getInputClass: Class[_] = {
    val existRdd = child.asInstanceOf[PhysicalRDD]
    val productClass = existRdd.rdd.firstParent.elementClassTag.runtimeClass
    logInfo("productClass is " + productClass)
    val clazz = productClass
    if (null == relation.rowClass) {
      relation.rowClass = clazz
    }
    clazz
  }

  private def getInspector(clazz: Class[_]): ObjectInspector = {
    val inspector = ObjectInspectorFactory.getReflectionObjectInspector(clazz,
      ObjectInspectorFactory.ObjectInspectorOptions.JAVA)
    inspector
  }

  private def initFieldInfo(): OrcSerde = {
    val serde: OrcSerde = new OrcSerde
    serde
  }

  /**
   * Inserts all rows into the Orc file.
   */
  override def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)

    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = job.getConfiguration

    val fspath = new Path(relation.path)
    val fs = fspath.getFileSystem(conf)

    if (overwrite) {
      try {
        fs.delete(fspath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${fspath.toString} prior"
              + s" to InsertIntoOrcTable:\n${e.toString}")
      }
    }

    val existRdd = child.asInstanceOf[PhysicalRDD]
    val parentRdd = existRdd.rdd.firstParent[Product]
    val writableRdd = parentRdd.mapPartitions { iter =>
      val objSnspector = ObjectInspectorFactory.getReflectionObjectInspector(
        getContextOrSparkClassLoader.loadClass(inputClass),
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA)
      iter.map(obj => orcSerde.serialize(obj, objSnspector))
    }

    saveAsHadoopFile(writableRdd, relation.rowClass, relation.path, conf)

    // We return the child RDD to allow chaining (alternatively, one could return nothing).
    childRdd
  }


  // based on ``saveAsNewAPIHadoopFile`` in [[PairRDDFunctions]]
  // TODO: Maybe PairRDDFunctions should use Product2 instead of Tuple2?
  // .. then we could use the default one and could use [[MutablePair]]
  // instead of ``Tuple2``
  private def saveAsHadoopFile(
      rdd: RDD[Writable],
      rowClass: Class[_],
      path: String,
      @transient conf: Configuration) {
    val job = new Job(conf)
    val keyType = classOf[Void]
    job.setOutputKeyClass(keyType)
    job.setOutputValueClass(classOf[Writable])
    FileOutputFormat.setOutputPath(job, new Path(path))

    val wrappedConf = new SerializableWritable(job.getConfiguration)

    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = sqlContext.sparkContext.newRddId()

    val taskIdOffset =
      if (overwrite) {
        1
      } else {
        FileSystemHelper
          .findMaxTaskId(
            FileOutputFormat.getOutputPath(job).toString, job.getConfiguration, "orc") + 1
      }

    def getWriter(
                   outFormat: OrcOutputFormat,
                   conf: Configuration,
                   path: Path,
                   reporter: Reporter) = {
      val fs = path.getFileSystem(conf)
      outFormat.getRecordWriter(fs, conf.asInstanceOf[JobConf], path.toUri.getPath, reporter).
        asInstanceOf[org.apache.hadoop.mapred.RecordWriter[NullWritable, Writable]]
    }

    def getCommitterAndWriter(offset: Int, context: TaskAttemptContext) = {
      val outFormat = new OrcOutputFormat

      val taskId: TaskID = context.getTaskAttemptID.getTaskID
      val partition: Int = taskId.getId
      val filename = s"part-r-${partition + offset}.orc"
      val output: Path = FileOutputFormat.getOutputPath(context)
      val committer = new FileOutputCommitter(output, context)
      val path = new Path(committer.getWorkPath, filename)
      val writer = getWriter(outFormat, wrappedConf.value, path, Reporter.NULL)
      (committer, writer)
    }

    def writeShard(context: TaskContext, iter: Iterator[Writable]): Int = {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        attemptNumber)
      val hadoopContext = newTaskAttemptContext(wrappedConf.value.asInstanceOf[JobConf], attemptId)
      val workerAndComitter = getCommitterAndWriter(taskIdOffset, hadoopContext)
      val writer = workerAndComitter._2

      while (iter.hasNext) {
        val row = iter.next()
        writer.write(NullWritable.get(), row)
      }

      writer.close(Reporter.NULL)
      workerAndComitter._1.commitTask(hadoopContext)
      return 1
    }

    /* apparently we need a TaskAttemptID to construct an OutputCommitter;
     * however we're only going to use this local OutputCommitter for
     * setupJob/commitJob, so we just use a dummy "map" task.
     */
    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(
      wrappedConf.value.asInstanceOf[JobConf], jobAttemptId)
    val workerAndComitter = getCommitterAndWriter(taskIdOffset, jobTaskContext)
    workerAndComitter._1.setupJob(jobTaskContext)
    sc.runJob(rdd, writeShard _)
    workerAndComitter._1.commitJob(jobTaskContext)
  }
}
