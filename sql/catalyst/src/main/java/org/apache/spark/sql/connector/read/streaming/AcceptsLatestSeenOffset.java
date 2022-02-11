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

package org.apache.spark.sql.connector.read.streaming;

/**
 * Indicates that the source accepts the latest seen offset, which requires streaming execution
 * to provide the latest seen offset when restarting the streaming query from checkpoint.
 *
 * Note that this interface aims to only support DSv2 streaming sources. Spark may throw error
 * if the interface is implemented along with DSv1 streaming sources.
 *
 * The callback method will be called once per run.
 */
public interface AcceptsLatestSeenOffset extends SparkDataStream {
  /**
   * Callback method to receive the latest seen offset information from streaming execution.
   * The method will be called only when the streaming query is restarted from checkpoint.
   *
   * @param offset The offset which was latest seen in the previous run.
   */
  void setLatestSeenOffset(Offset offset);
}
