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

import java.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.HiveShim
import org.apache.spark.sql.sources.Filter
import org.apache.spark.{Logging, SerializableWritable}

/* Implicit conversions */
import scala.collection.JavaConversions._

private[orc] case class OrcTableScan(attributes: Seq[Attribute],
    @transient relation: OrcRelation,
    filters: Array[Filter],
    inputPaths: Array[String]) extends Logging {
  @transient private val sqlContext = relation.sqlContext

  private def addColumnIds(
      output: Seq[Attribute],
      relation: OrcRelation,
      conf: Configuration): Unit = {
    val ids = output.map(a => relation.dataSchema.fieldIndex(a.name): Integer)
    val (sortedIds, sortedNames) = ids.zip(attributes.map(_.name)).sorted.unzip
    HiveShim.appendReadColumns(conf, sortedIds, sortedNames)
  }

  private def buildFilter(job: Job, filters: Array[Filter]): Unit = {
    if (ORC_FILTER_PUSHDOWN_ENABLED) {
      val conf: Configuration = job.getConfiguration
      OrcFilters.createFilter(filters).foreach { f =>
        conf.set(SARG_PUSHDOWN, toKryo(f))
        conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
      }
    }
  }

  // Transform all given raw `Writable`s into `Row`s.
  private def fillObject(
      path: String,
      conf: Configuration,
      iterator: Iterator[org.apache.hadoop.io.Writable],
      nonPartitionKeyAttrs: Seq[(Attribute, Int)],
      mutableRow: MutableRow): Iterator[Row] = {
    val deserializer = new OrcSerde
    val soi = OrcFileOperator.getObjectInspector(path, Some(conf))
    val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map {
      case (attr, ordinal) =>
        soi.getStructFieldRef(attr.name.toLowerCase) -> ordinal
    }.unzip
    val unwrappers = HadoopTypeConverter.unwrappers(fieldRefs)
    // Map each tuple to a row object
    iterator.map { value =>
      val raw = deserializer.deserialize(value)
      var i = 0
      while (i < fieldRefs.length) {
        val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
        if (fieldValue == null) {
          mutableRow.setNullAt(fieldOrdinals(i))
        } else {
          unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
        }
        i += 1
      }
      mutableRow: Row
    }
  }

  def execute(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val job = new Job(sc.hadoopConfiguration)
    val conf: Configuration = job.getConfiguration

    buildFilter(job, filters)
    addColumnIds(attributes, relation, conf)
    FileInputFormat.setInputPaths(job, inputPaths.map(new Path(_)): _*)

    val inputClass = classOf[OrcInputFormat].asInstanceOf[
      Class[_ <: org.apache.hadoop.mapred.InputFormat[NullWritable, Writable]]]

    val rdd = sc.hadoopRDD(conf.asInstanceOf[JobConf],
      inputClass, classOf[NullWritable], classOf[Writable])
      .asInstanceOf[HadoopRDD[NullWritable, Writable]]
    val wrappedConf = new SerializableWritable(conf)
    val rowRdd: RDD[Row] = rdd.mapPartitionsWithInputSplit { case (split: OrcSplit, iter) =>
      val pathStr = split.getPath.toString
      val mutableRow = new SpecificMutableRow(attributes.map(_.dataType))
      fillObject(pathStr, wrappedConf.value, iter.map(_._2), attributes.zipWithIndex, mutableRow)
    }
    rowRdd
  }
}
