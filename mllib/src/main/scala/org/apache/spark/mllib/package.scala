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

package org.apache.spark

/**
 * RDD-based machine learning APIs (in maintenance mode).
 *
 * The `spark.mllib` package is in maintenance mode as of the Spark 2.0.0 release to encourage
 * migration to the DataFrame-based APIs under the [[org.apache.spark.ml]] package.
 * While in maintenance mode,
 *
 *  - no new features in the RDD-based `spark.mllib` package will be accepted, unless they block
 *    implementing new features in the DataFrame-based `spark.ml` package;
 *  - bug fixes in the RDD-based APIs will still be accepted.
 *
 * The developers will continue adding more features to the DataFrame-based APIs in the 2.x series
 * to reach feature parity with the RDD-based APIs.
 * And once we reach feature parity, this package will be deprecated.
 *
 * @see <a href="https://issues.apache.org/jira/browse/SPARK-4591">SPARK-4591</a> to track
 * the progress of feature parity
 */
package object mllib
