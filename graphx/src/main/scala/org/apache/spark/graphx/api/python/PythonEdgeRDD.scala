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

package org.apache.spark.graphx.api.python

import java.util.{List => JList, Map => JMap}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.storage.StorageLevel

private[graphx] class PythonEdgeRDD (
    parent: JavaRDD[Array[Byte]],
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    preservePartitioning: Boolean,
    pythonExec: String,
    partitionStrategy: String,
    targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends EdgeRDD[Array[Byte], Array[Byte]](parent.firstParent, targetStorageLevel) {

  }
