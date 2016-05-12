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
package org.apache.spark.sql.hive.execution

import org.apache.hadoop.mapred.FileOutputFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.hive.HiveShim.ShimFileSinkDesc
import org.apache.spark.sql.hive.SparkHiveWriterContainer
import org.apache.spark.util.SerializableJobConf

// Base trait from which all hive insert statement physical execution extends.
private[hive] trait SaveAsHiveFile extends UnaryExecNode {

protected def saveAsHiveFile(
  rdd: RDD[InternalRow],
  valueClass: Class[_],
  fileSinkConf: ShimFileSinkDesc,
  conf: SerializableJobConf,
  writerContainer: SparkHiveWriterContainer,
  isCompressed: Boolean): Unit = {
    assert(valueClass != null, "Output value class not set")

    if (isCompressed) {
      // Please note that isCompressed, "mapred.output.compress", "mapred.output.compression.codec",
      // and "mapred.output.compression.type" have no impact on ORC because it uses table properties
      // to store compression information.
      conf.value.set("mapred.output.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(conf.value.get("mapred.output.compression.codec"))
      fileSinkConf.setCompressType(conf.value.get("mapred.output.compression.type"))
    }

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    val speculationEnabled = sqlContext.sparkContext.conf.getBoolean("spark.speculation", false)
    val outputCommitterClass = conf.value.get("mapred.output.committer.class", "")
    if (speculationEnabled && outputCommitterClass.contains("Direct")) {
      val warningMessage =
        s"$outputCommitterClass may be an output committer that writes data directly to " +
          "the final location. Because speculation is enabled, this output committer may " +
          "cause data loss (see the case in SPARK-10063). If possible, please use a output " +
          "committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }
    conf.value.setOutputValueClass(valueClass)


    val outputFileFormatClassName = fileSinkConf.getTableInfo.getOutputFileFormatClassName
    assert(outputFileFormatClassName != null, "Output format class not set")
    conf.value.set("mapred.output.format.class", outputFileFormatClassName)

    FileOutputFormat.setOutputPath(
      conf.value,
      SparkHiveWriterContainer.createPathFromString(fileSinkConf.getDirName(), conf.value))
    log.debug("Saving as hadoop file of type " + valueClass.getSimpleName)
    writerContainer.driverSideSetup()
    sqlContext.sparkContext.runJob(rdd, writerContainer.writeToFile _)
    writerContainer.commitJob()
    }
}
