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
package org.apache.spark.streaming.flume.sink

private[flume] object SparkSinkUtils {
  /**
   * This method determines if this batch represents an error or not.
   * @param batch - The batch to check
   * @return - true if the batch represents an error
   */
  def isErrorBatch(batch: EventBatch): Boolean = {
    !batch.getErrorMsg.toString.equals("") // If there is an error message, it is an error batch.
  }
}
