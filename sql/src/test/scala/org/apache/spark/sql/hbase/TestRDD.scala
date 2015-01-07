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

package org.apache.spark.sql.hbase

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

class TestRDD(parent : RDD[String], happyFace : String, nPartitions: Int)
  extends RDD[String](parent) {

  @transient val logger = Logger.getLogger(getClass.getName)
  val parentDebugString = parent.toDebugString

  def myHappyFace = happyFace

  override def compute(split: Partition, context: TaskContext): Iterator[String]
  = List(s"My partition is ${split.index} says parent is /* $parentDebugString").iterator

  override protected def getPartitions: Array[Partition] = Array.tabulate[Partition](nPartitions){
    pindex : Int => new Partition() { def index = pindex }}
}

object TestRdd {
  def test() = {
    //val myrdd = sc.parallelize( (0 until 100).map{ n => s"Hi there $n"},2)
    val NPartitions = 10
    val sc = null.asInstanceOf[SparkContext]
    val myrdd = sc.parallelize( (0 until 100).map{ n => s"Hi there $n"}, NPartitions)
     val myTestRdd = new TestRDD(myrdd,"MyHappyFace", NPartitions)

    import java.io._

    val objFile = "/tmp/rdd.out"
    val fos = new FileOutputStream(objFile)
    val oos = new ObjectOutputStream(fos)
    val mySerializedRdd = oos.writeObject(myTestRdd)
    val fis = new FileInputStream(objFile)
    val ois = new ObjectInputStream(fis)
    val myNewSerializedRdd = ois.readObject
    val collector = myNewSerializedRdd.asInstanceOf[TestRDD]
    println(s"Collector class is ${collector.getClass.getName}")
    println("%s".format(collector.getClass.getMethods.mkString("Methods: [",",","]")))
    println(s"Collector is ${collector.toDebugString}")
    println(s"Collect output: ${collector.collect()}")
    myNewSerializedRdd
  }
}

//TestRdd.test
