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

package org.apache.spark.sql.hive

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.hadoop.hive.serde2.objectinspector.{StructObjectInspector, ObjectInspectorUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.{ ShimFileSinkDesc => FileSinkDesc }
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.{Logging, SparkContext, TaskContext, SerializableWritable}

/**
 *  A trait for subclasses that write data using arbitrary SerDes to a file system .
 */
private[hive] trait SaveAsHiveFile extends HiveInspectors with Logging {
  def newSerializer(tableDesc: TableDesc): Serializer = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, tableDesc.getProperties)
    serializer
  }

  def saveAsHiveFile(
      sparkContext: SparkContext,
      rdd: RDD[Row],
      valueClass: Class[_],
      fileSinkConf: FileSinkDesc,
      conf: SerializableWritable[JobConf],
      writerContainer: SparkHiveWriterContainer): Unit = {
    assert(valueClass != null, "Output value class not set")
    conf.value.setOutputValueClass(valueClass)

    val outputFileFormatClassName = fileSinkConf.getTableInfo.getOutputFileFormatClassName
    assert(outputFileFormatClassName != null, "Output format class not set")
    conf.value.set("mapred.output.format.class", outputFileFormatClassName)

    FileOutputFormat.setOutputPath(
      conf.value,
      SparkHiveWriterContainer.createPathFromString(fileSinkConf.getDirName, conf.value))
    log.debug("Saving as hadoop file of type " + valueClass.getSimpleName)

    writerContainer.driverSideSetup()
    sparkContext.runJob(rdd, writeToFile _)
    writerContainer.commitJob()

    // Note that this function is executed on executor side
    def writeToFile(context: TaskContext, iterator: Iterator[Row]): Unit = {
      val serializer = newSerializer(fileSinkConf.getTableInfo)
      val standardOI = ObjectInspectorUtils
        .getStandardObjectInspector(
          fileSinkConf.getTableInfo.getDeserializer.getObjectInspector,
          ObjectInspectorCopyOption.JAVA)
        .asInstanceOf[StructObjectInspector]

      val fieldOIs = standardOI.getAllStructFieldRefs.map(_.getFieldObjectInspector).toArray
      val wrappers = fieldOIs.map(wrapperFor)
      val outputData = new Array[Any](fieldOIs.length)

      writerContainer.executorSideSetup(context.stageId, context.partitionId, context.attemptNumber)

      iterator.foreach { row =>
        var i = 0
        while (i < fieldOIs.length) {
          outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row(i))
          i += 1
        }

        writerContainer
          .getLocalFileWriter(row)
          .write(serializer.serialize(outputData, standardOI))
      }

      writerContainer.close()
    }
  }
}
