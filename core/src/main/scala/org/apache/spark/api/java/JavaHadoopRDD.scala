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

package org.apache.spark.api.java

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.mapred.InputSplit

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaSparkContext._
import org.apache.spark.api.java.function.{Function2 => JFunction2}
import org.apache.spark.rdd.HadoopRDD

@DeveloperApi
class JavaHadoopRDD[K, V](rdd: HadoopRDD[K, V])
    (implicit override val kClassTag: ClassTag[K], implicit override val vClassTag: ClassTag[V])
  extends JavaPairRDD[K, V](rdd) {

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[R](
      f: JFunction2[InputSplit, java.util.Iterator[(K, V)], java.util.Iterator[R]],
      preservesPartitioning: Boolean = false): JavaRDD[R] = {
    new JavaRDD(rdd.mapPartitionsWithInputSplit((a, b) => f.call(a, b.asJava).asScala,
      preservesPartitioning)(fakeClassTag))(fakeClassTag)
  }
}
