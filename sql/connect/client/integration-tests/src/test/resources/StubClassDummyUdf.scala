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
package org.apache.spark.sql.connect.client

// To generate a jar from the source file:
// `scalac StubClassDummyUdf.scala -d udf.jar`
// To remove class A from the jar:
// `jar -xvf udf.jar` -> delete A.class and A$.class
// `jar -cvf udf_noA.jar org/`
class StubClassDummyUdf {
  val udf: Int => Int = (x: Int) => x + 1
  val dummy = (x: Int) => A(x)
}

case class A(x: Int) { def get: Int = x + 5 }

// The code to generate the udf file
object StubClassDummyUdf {
  import java.io.{BufferedOutputStream, File, FileOutputStream}
  import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.PrimitiveIntEncoder
  import org.apache.spark.sql.connect.common.UdfPacket
  import org.apache.spark.util.Utils

  def packDummyUdf(): String = {
    val byteArray =
      Utils.serialize[UdfPacket](
        new UdfPacket(
          new StubClassDummyUdf().udf,
          Seq(PrimitiveIntEncoder),
          PrimitiveIntEncoder
        )
      )
    val file = new File("src/test/resources/udf")
    val target = new BufferedOutputStream(new FileOutputStream(file))
    try {
      target.write(byteArray)
      file.getAbsolutePath
    } finally {
      target.close
    }
  }
}
