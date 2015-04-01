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
package org.apache.spark.crypto

import org.apache.hadoop.io.Text

/**
 * Constant variables
 */
object CommonConfigurationKeys {
  val SPARK_SHUFFLE_TOKEN: Text = new Text("SPARK_SHUFFLE_TOKEN")
  val SPARK_ENCRYPTED_INTERMEDIATE_DATA_BUFFER_KB: String = "spark.job" +
    ".encrypted-intermediate-data.buffer.kb"
  val DEFAULT_SPARK_ENCRYPTED_INTERMEDIATE_DATA_BUFFER_KB: Int = 128
  val SPARK_ENCRYPTED_INTERMEDIATE_DATA: String =
    "spark.job.encrypted-intermediate-data"
  val DEFAULT_SPARK_ENCRYPTED_INTERMEDIATE_DATA: Boolean = false
  val SPARK_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS: String =
    "spark.job.encrypted-intermediate-data-key-size-bits"
  val DEFAULT_SPARK_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS: Int = 128
}
