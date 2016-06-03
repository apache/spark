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

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.TestSparkSession

/**
 * Test suite to test Kryo custom registrators.
 */
class DatasetSerializerRegistratorSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  /**
   * Initialize the [[TestSparkSession]] with a [[KryoRegistrator]].
   */
  protected override def beforeAll(): Unit = {
    sparkConf.set("spark.kryo.registrator", TestRegistrator().getClass.getCanonicalName)
    super.beforeAll()
  }

  test("Kryo registrator") {
    implicit val kryoEncoder = Encoders.kryo[KryoData]
    val ds = Seq(KryoData(1), KryoData(2)).toDS()
    assert(ds.collect().toSet == Set(KryoData(0), KryoData(0)))
  }

}

/** Used to test user provided registrator. */
class TestRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit =
    kryo.register(classOf[KryoData], new ZeroKryoDataSerializer())
}

object TestRegistrator {
  def apply(): TestRegistrator = new TestRegistrator()
}

/** A [[Serializer]] that takes a [[KryoData]] and serializes it as KryoData(0). */
class ZeroKryoDataSerializer extends Serializer[KryoData] {
  override def write(kryo: Kryo, output: Output, t: KryoData): Unit = {
    output.writeInt(0)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[KryoData]): KryoData = {
    KryoData(input.readInt())
  }
}
