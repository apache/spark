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

package org.apache.spark.serializer;

import java.io.NotSerializableException

import org.scalatest.FunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.spark.SharedSparkContext

/* A trivial (but unserializable) container for trivial functions */
class UnserializableClass {
  def op[T](x: T) = x.toString
  
  def pred[T](x: T) = x.toString.length % 2 == 0
}

class ProactiveClosureSerializationSuite extends FunSuite with SharedSparkContext {

  def fixture = (sc.parallelize(0 until 1000).map(_.toString), new UnserializableClass)

  test("throws expected serialization exceptions on actions") {
    val (data, uc) = fixture
      
    val ex = intercept[SparkException] {
      data.map(uc.op(_)).count
    }
        
    assert(ex.getMessage.matches(".*Task not serializable.*"))
  }

  // There is probably a cleaner way to eliminate boilerplate here, but we're
  // iterating over a map from transformation names to functions that perform that
  // transformation on a given RDD, creating one test case for each
  
  for (transformation <- 
      Map("map" -> map _, "flatMap" -> flatMap _, "filter" -> filter _, "mapWith" -> mapWith _,
          "mapPartitions" -> mapPartitions _, "mapPartitionsWithIndex" -> mapPartitionsWithIndex _,
          "mapPartitionsWithContext" -> mapPartitionsWithContext _, "filterWith" -> filterWith _)) {
    val (name, xf) = transformation
    
    test(s"$name transformations throw proactive serialization exceptions") {
      val (data, uc) = fixture
      
      val ex = intercept[SparkException] {
        xf(data, uc)
      }

      assert(ex.getMessage.matches(".*Task not serializable.*"), s"RDD.$name doesn't proactively throw NotSerializableException")
    }
  }
  
  def map(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.map(y => uc.op(y))

  def mapWith(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.mapWith(x => x.toString)((x,y) => x + uc.op(y))
    
  def flatMap(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.flatMap(y=>Seq(uc.op(y)))
  
  def filter(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.filter(y=>uc.pred(y))
  
  def filterWith(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.filterWith(x => x.toString)((x,y) => uc.pred(y))
  
  def mapPartitions(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.mapPartitions(_.map(y => uc.op(y)))
  
  def mapPartitionsWithIndex(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.mapPartitionsWithIndex((_, it) => it.map(y => uc.op(y)))
  
  def mapPartitionsWithContext(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.mapPartitionsWithContext((_, it) => it.map(y => uc.op(y)))
  
}
