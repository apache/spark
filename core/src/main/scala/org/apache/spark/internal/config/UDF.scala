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
package org.apache.spark.internal.config

private[spark] object UDF {

  val DISPATCHER_FACTORY = ConfigBuilder("spark.udf.worker.dispatcherFactory")
    .doc("Name of the class implementing " +
      "org.apache.spark.udf.worker.core.UDFDispatcherFactory, used to create the " +
      "dispatchers that provision and manage external UDF workers (SPARK-55278). The " +
      "class must have a no-arg constructor, a constructor taking a SparkConf, or a " +
      "constructor taking a SparkConf and a Boolean indicating whether it is being " +
      "created on the driver. When unset, no external UDF dispatcher is available and " +
      "executing an external UDF fails. This is an operator-level setting: it selects " +
      "the runtime that user code is executed by, so it is not intended to be changed " +
      "by individual queries.")
    .version("4.4.0")
    .stringConf
    .createOptional
}
