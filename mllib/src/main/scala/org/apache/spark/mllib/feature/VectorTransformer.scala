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

package org.apache.spark.mllib.feature

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Trait for transformation of a vector
 */
@Since("1.1.0")
@DeveloperApi
trait VectorTransformer extends Serializable {

  /**
   * Applies transformation on a vector.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
   */
  @Since("1.1.0")
  def transform(vector: Vector): Vector

  /**
   * Applies transformation on an RDD[Vector].
   *
   * @param data RDD[Vector] to be transformed.
   * @return transformed RDD[Vector].
   */
  @Since("1.1.0")
  def transform(data: RDD[Vector]): RDD[Vector] = {
    // Later in #1498 , all RDD objects are sent via broadcasting instead of RPC.
    // So it should be no longer necessary to explicitly broadcast `this` object.
    data.map(x => this.transform(x))
  }

  /**
   * Applies transformation on a JavaRDD[Vector].
   *
   * @param data JavaRDD[Vector] to be transformed.
   * @return transformed JavaRDD[Vector].
   */
  @Since("1.1.0")
  def transform(data: JavaRDD[Vector]): JavaRDD[Vector] = {
    transform(data.rdd)
  }

}
