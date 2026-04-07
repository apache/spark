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
package org.apache.spark.sql.connect

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * A mock SparkContext for Spark Connect testing. It extends a real SparkContext
 * with minimal local configuration, but overrides `parallelize` to throw an
 * unsupported operation error, since RDD operations are not supported in Connect.
 */
private[sql] class MockSparkContext extends SparkContext(
    new SparkConf()
      .setMaster("local[1]")
      .setAppName("mock-connect-sparkcontext")
      .set("spark.ui.enabled", "false")) {

  override def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] =
    throw ConnectClientUnsupportedErrors.sparkContext()
}

private[sql] object MockSparkContext {
  lazy val instance: MockSparkContext = new MockSparkContext()
}
