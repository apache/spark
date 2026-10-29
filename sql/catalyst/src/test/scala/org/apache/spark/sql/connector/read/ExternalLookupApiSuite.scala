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

package org.apache.spark.sql.connector.read

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types.{IntegerType, StructType}

class ExternalLookupApiSuite extends SparkFunSuite {

  private val offeredPredicate: Predicate =
    new Predicate("=", Array[Expression](
      FieldReference.column("id"), new LookupParameter(IntegerType)))

  private def javaRoundTrip[T: ClassTag](value: T): T = {
    val serializer = new JavaSerializer(new SparkConf()).newInstance()
    serializer.deserialize[T](serializer.serialize(value))
  }

  test("a lookup parameter is a leaf rendered as ?") {
    val parameter = new LookupParameter(IntegerType)
    assert(parameter.children().isEmpty)
    assert(parameter.references().isEmpty)
    assert(parameter.describe() === "?")
    assert(offeredPredicate.describe() === "id = ?")
    assert(offeredPredicate.toString === "id = ?")
  }

  test("an offered predicate and a reader factory that retains it are Java serializable") {
    val predicate = javaRoundTrip(offeredPredicate)
    assert(predicate.describe() === "id = ?")
    assert(predicate.children()(1).asInstanceOf[LookupParameter].dataType() === IntegerType)

    val factory = javaRoundTrip(RetainingLookupReaderFactory(Seq(offeredPredicate)))
    assert(factory.selected.map(_.describe()) === Seq("id = ?"))
    assert(factory.selected.head.children()(1).isInstanceOf[LookupParameter])
  }

  test("a scan without toExternalLookup reports the named error") {
    val scan = new Scan {
      override def readSchema(): StructType = new StructType()
      override def description(): String = "TestScan"
    }
    checkError(
      exception = intercept[SparkUnsupportedOperationException] { scan.toExternalLookup() },
      condition = "DATA_SOURCE_EXTERNAL_LOOKUP_NOT_SUPPORTED",
      parameters = Map("description" -> "TestScan"))
  }
}

/** A reader factory that keeps the connector's selected predicates for use on executors. */
case class RetainingLookupReaderFactory(selected: Seq[Predicate])
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = throw new UnsupportedOperationException()
}
