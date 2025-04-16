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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input

import org.apache.spark.{SparkConf, SparkIllegalArgumentException}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.columnar.{DefaultCachedBatch, DefaultCachedBatchKryoSerializer}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.storage.StorageLevel

class CacheTableInKryoSuite extends QueryTest
  with SQLTestUtils
  with SharedSparkSession {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  test("SPARK-51777: sql.columnar.* classes registered in KryoSerializer") {
    withTable("t1") {
      sql("CREATE TABLE t1 AS SELECT 1 AS a")
      checkAnswer(sql("SELECT * FROM t1").persist(StorageLevel.DISK_ONLY), Seq(Row(1)))
    }
  }

  test("SPARK-51790: UTF8String should be registered in KryoSerializer") {
    withTable("t1") {
      sql(
        """
          |CREATE TABLE t1 AS
          |  SELECT * from
          |  VALUES ('apache', 'spark', 'community'),
          |         ('Apache', 'Spark', 'Community')
          |  as t(a, b, c)
          |""".stripMargin)
        checkAnswer(sql("SELECT a, b, c FROM t1").persist(StorageLevel.DISK_ONLY),
            Seq(Row("apache", "spark", "community"), Row("Apache", "Spark", "Community")))
    }
  }

  test("SPARK-51813 DefaultCachedBatchKryoSerializer do not propagate nulls") {
    val ks = new KryoSerializer(this.sparkConf)
    val kryo = ks.newKryo()
    val serializer = kryo.getDefaultSerializer(classOf[DefaultCachedBatch])
    assert(serializer.isInstanceOf[DefaultCachedBatchKryoSerializer])
    val ser = serializer.asInstanceOf[DefaultCachedBatchKryoSerializer]

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        ser.write(kryo, ks.newKryoOutput(), DefaultCachedBatch(1, null, InternalRow.empty))
      },
      condition = "INVALID_KRYO_SERIALIZER_NO_DATA",
      parameters = Map(
        "obj" -> "DefaultCachedBatch.buffers",
        "serdeOp" -> "serialize",
        "serdeClass" -> ser.getClass.getName))

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        ser.write(kryo, ks.newKryoOutput(),
          DefaultCachedBatch(1, Seq(Array.empty[Byte], null).toArray, InternalRow.empty))
      },
      condition = "INVALID_KRYO_SERIALIZER_NO_DATA",
      parameters = Map(
        "obj" -> "DefaultCachedBatch.buffers(1)",
        "serdeOp" -> "serialize",
        "serdeClass" -> ser.getClass.getName))

    val output1 = ks.newKryoOutput()
    output1.writeInt(1) // numRows
    output1.writeInt(Kryo.NULL) // malformed buffers.length

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        ser.read(kryo, new Input(output1.toBytes), classOf[DefaultCachedBatch])
      },
      condition = "INVALID_KRYO_SERIALIZER_NO_DATA",
      parameters = Map(
        "obj" -> "DefaultCachedBatch.buffers",
        "serdeOp" -> "deserialize",
        "serdeClass" -> ser.getClass.getName))
    output1.close()

    val output2 = ks.newKryoOutput()
    output2.writeInt(1) // numRows
    output2.writeInt(3) // buffers.length + 1
    output2.writeInt(Kryo.NULL) // malformed buffers[0].length
    output2.writeBytes(Array[Byte](1, 2, 3)) // buffers[0]

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        ser.read(kryo, new Input(output2.toBytes, 0, 14), classOf[DefaultCachedBatch])
      },
      condition = "INVALID_KRYO_SERIALIZER_NO_DATA",
      parameters = Map(
        "obj" -> "DefaultCachedBatch.buffers(0)",
        "serdeOp" -> "deserialize",
        "serdeClass" -> ser.getClass.getName))
  }
}
