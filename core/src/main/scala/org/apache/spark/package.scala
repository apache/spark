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

package org.apache

/**
 * Core Spark functionality. [[org.apache.spark.SparkContext]] serves as the main entry point to
 * Spark, while [[org.apache.spark.rdd.RDD]] is the data type representing a distributed collection,
 * and provides most parallel operations.
 *
 * In addition, [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs
 * of key-value pairs, such as `groupByKey` and `join`; [[org.apache.spark.rdd.DoubleRDDFunctions]]
 * contains operations available only on RDDs of Doubles; and
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that can
 * be saved as SequenceFiles. These operations are automatically available on any RDD of the right
 * type (e.g. RDD[(Int, Int)] through implicit conversions when you
 * `import org.apache.spark.SparkContext._`.
 *
 * Java programmers should reference the [[spark.api.java]] package
 * for Spark programming APIs in Java.
 */
package object spark {
  // For package docs only
}
