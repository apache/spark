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

package org.apache.spark.ml.util

import org.apache.spark.annotation.Since
import org.apache.spark.util.KnownSizeEstimation

/**
 * For ml connect only.
 * Trait for the Summary
 * All the summaries should extend from this Summary in order to
 * support connect.
 */
@Since("4.0.0")
private[spark] trait Summary extends KnownSizeEstimation {

  // A summary is normally a small object, with several RDDs or DataFrame.
  // The SizeEstimator is likely to overestimate the size of the summary,
  // because it will also count the underlying SparkSession and/or SparkContext,
  // which mainly contributes to the size of the summary.
  // So we set the default size as 256KB.
  override def estimatedSize: Long = 256 * 1024
}
