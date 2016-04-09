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

package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkContext}

object SparkSimpleExample extends App{
  var conf = new SparkConf()
  conf.setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  // Encoders for most common types are automatically provided by importing sqlContext.implicits._
  // val ds = Seq(1, 2, 3).toDS()
  // ds.map(_ + 1).collect() // Returns: Array(2, 3, 4)

  // Encoders are also created for case classes.
  case class Person(name: String, age: Long)
  val ds = sqlContext.createDataset(Seq(Person("Andy", 32)))
  ds.show()

  // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
  // val path = "examples/src/main/resources/people.json"
  // val people = sqlContext.read.json(path).as[Person]
}
