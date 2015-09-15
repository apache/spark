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

import org.apache.spark.{SparkConf, SparkFunSuite}

class JavaSerializerSuite extends SparkFunSuite {
  test("JavaSerializer instances are serializable") {
    val serializer = new JavaSerializer(new SparkConf())
    val instance = serializer.newInstance()
    instance.deserialize[JavaSerializer](instance.serialize(serializer))
  }

  test("Deserialize object containing a primitive Class as attribute") {
    val serializer = new JavaSerializer(new SparkConf())
    val instance = serializer.newInstance()
    instance.deserialize[JavaSerializer](instance.serialize(new ContainsPrimitiveClass()))
  }
}

private class ContainsPrimitiveClass extends Serializable {
  val intClass = classOf[Int]
  val longClass = classOf[Long]
  val shortClass = classOf[Short]
  val charClass = classOf[Char]
  val doubleClass = classOf[Double]
  val floatClass = classOf[Float]
  val booleanClass = classOf[Boolean]
  val byteClass = classOf[Byte]
  val voidClass = classOf[Void]
}
