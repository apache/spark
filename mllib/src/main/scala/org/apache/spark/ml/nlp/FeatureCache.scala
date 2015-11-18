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
package org.apache.spark.ml.nlp
import scala.collection.mutable.ArrayBuffer

class FeatureCache {
  private var feature_freelist: ArrayBuffer[Int] = new ArrayBuffer[Int]()

  def shrink(old2new: Map[Integer, Integer]): Unit = {

  }

  def add(f: ArrayBuffer[Int]): Unit = {
    feature_freelist ++= f
    // feature_freelist :+ -1 // ...until *f!=-1 end mark sentinel
  }
}