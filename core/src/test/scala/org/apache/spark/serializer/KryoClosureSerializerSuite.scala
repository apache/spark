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

package org.apache.spark.serializer

import java.io.ByteArrayOutputStream

import scala.collection.mutable
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.Kryo
import org.scalatest.FunSuite

import org.apache.spark.{SharedSparkContext, SparkConf, Partition, SerializableWritable}
import org.apache.spark.scheduler.HighlyCompressedMapStatus
import org.apache.spark.serializer.KryoTest._
import org.apache.spark.storage.BlockManagerId
import com.esotericsoftware.minlog.{Log => MinLog}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.scheduler.ResultTask


class TestPartition(@transient fs: FileSplit) extends Partition {

  override def index = 42
  val foobar = new SerializableWritable[InputSplit](fs)
}

class KryoClosureSerializerSuite extends FunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
  

  test("serialize various things using kryo closure serializer") {
    val kryo = new KryoSerializer(conf).newInstance()
    val fsplit = new FileSplit(new Path("/foo"), 0l, 100l, Array("host1"))
    //val part = new HadoopPartition(1, 2, fsplit)
    val part = new TestPartition(fsplit)
        val bcast = sc.broadcast(new Array[Byte](4))

    MinLog.set(MinLog.LEVEL_TRACE)
    //val serialized = kryo.serialize(part)
    //println(s"serialized.limit=${serialized.limit}")
    //val des = kryo.deserialize[Partition](serialized).asInstanceOf[TestPartition]
    //println(s"is=${des.foobar}")

    val task = new ResultTask[Int, Int](1, bcast, part, null, 1)

    val s = kryo.serialize(task)
    println(s"limit=${s.limit}")
    val d = kryo.deserialize[ResultTask[Int, Int]](s)
  }
}
