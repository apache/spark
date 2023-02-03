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

package org.apache.spark.internal.io.cloud
import java.io.File

import org.apache.spark.sql.sources.PartitionedWriteSuite
import org.apache.spark.SparkConf
import org.apache.spark.internal.io.cloud.CommitterBindingSuite.COMMITTER_OPTIONS
import org.apache.spark.internal.io.cloud.PathOutputCommitProtocol.REPORT_DIR

/**
 * Extend the sql core PartitionedWriteSuite with a binding to
 * the FileOutputCommitter through the PathOutputCommitter.
 * This verifies consistency of job commit with the original Hadoop
 * binding.
 */
class PathOutputPartitionedWriteSuite extends PartitionedWriteSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .setAll(COMMITTER_OPTIONS)
      // .set("pathoutputcommit.reject.fileoutput", "true")
   //   .set("mapreduce.fileoutputcommitter.algorithm.version", "3")
      .set(REPORT_DIR, new File("./reports").getCanonicalFile.toURI.toString)
  }
}
