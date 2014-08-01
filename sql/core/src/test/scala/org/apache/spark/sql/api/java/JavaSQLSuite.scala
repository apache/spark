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

package org.apache.spark.sql.api.java

import scala.beans.BeanProperty

import org.scalatest.FunSuite

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.TestSQLContext

// Implicits
import scala.collection.JavaConversions._

class PersonBean extends Serializable {
  @BeanProperty
  var name: String = _

  @BeanProperty
  var age: Int = _
}

class AllTypesBean extends Serializable {
  @BeanProperty var stringField: String = _
  @BeanProperty var intField: java.lang.Integer = _
  @BeanProperty var longField: java.lang.Long = _
  @BeanProperty var floatField: java.lang.Float = _
  @BeanProperty var doubleField: java.lang.Double = _
  @BeanProperty var shortField: java.lang.Short = _
  @BeanProperty var byteField: java.lang.Byte = _
  @BeanProperty var booleanField: java.lang.Boolean = _
}

class JavaSQLSuite extends FunSuite {
  val javaCtx = new JavaSparkContext(TestSQLContext.sparkContext)
  val javaSqlCtx = new JavaSQLContext(javaCtx)

  test("schema from JavaBeans") {
    val person = new PersonBean
    person.setName("Michael")
    person.setAge(29)

    val rdd = javaCtx.parallelize(person :: Nil)
    val schemaRDD = javaSqlCtx.applySchema(rdd, classOf[PersonBean])

    schemaRDD.registerAsTable("people")
    javaSqlCtx.sql("SELECT * FROM people").collect()
  }

  test("all types in JavaBeans") {
    val bean = new AllTypesBean
    bean.setStringField("")
    bean.setIntField(0)
    bean.setLongField(0)
    bean.setFloatField(0.0F)
    bean.setDoubleField(0.0)
    bean.setShortField(0.toShort)
    bean.setByteField(0.toByte)
    bean.setBooleanField(false)

    val rdd = javaCtx.parallelize(bean :: Nil)
    val schemaRDD = javaSqlCtx.applySchema(rdd, classOf[AllTypesBean])
    schemaRDD.registerAsTable("allTypes")

    assert(
      javaSqlCtx.sql(
        """
          |SELECT stringField, intField, longField, floatField, doubleField, shortField, byteField,
          |       booleanField
          |FROM allTypes
        """.stripMargin).collect.head.row ===
      Seq("", 0, 0L, 0F, 0.0, 0.toShort, 0.toByte, false))
  }

  test("all types null in JavaBeans") {
    val bean = new AllTypesBean
    bean.setStringField(null)
    bean.setIntField(null)
    bean.setLongField(null)
    bean.setFloatField(null)
    bean.setDoubleField(null)
    bean.setShortField(null)
    bean.setByteField(null)
    bean.setBooleanField(null)

    val rdd = javaCtx.parallelize(bean :: Nil)
    val schemaRDD = javaSqlCtx.applySchema(rdd, classOf[AllTypesBean])
    schemaRDD.registerAsTable("allTypes")

    assert(
      javaSqlCtx.sql(
        """
          |SELECT stringField, intField, longField, floatField, doubleField, shortField, byteField,
          |       booleanField
          |FROM allTypes
        """.stripMargin).collect.head.row ===
        Seq.fill(8)(null))
  }

  test("loads JSON datasets") {
    val jsonString =
      """{"string":"this is a simple string.",
          "integer":10,
          "long":21474836470,
          "bigInteger":92233720368547758070,
          "double":1.7976931348623157E308,
          "boolean":true,
          "null":null
      }""".replaceAll("\n", " ")
    val rdd = javaCtx.parallelize(jsonString :: Nil)

    var schemaRDD = javaSqlCtx.jsonRDD(rdd)

    schemaRDD.registerAsTable("jsonTable1")

    assert(
      javaSqlCtx.sql("select * from jsonTable1").collect.head.row ===
        Seq(BigDecimal("92233720368547758070"),
            true,
            1.7976931348623157E308,
            10,
            21474836470L,
            null,
            "this is a simple string."))

    val file = getTempFilePath("json")
    val path = file.toString
    rdd.saveAsTextFile(path)
    schemaRDD = javaSqlCtx.jsonFile(path)

    schemaRDD.registerAsTable("jsonTable2")

    assert(
      javaSqlCtx.sql("select * from jsonTable2").collect.head.row ===
        Seq(BigDecimal("92233720368547758070"),
            true,
            1.7976931348623157E308,
            10,
            21474836470L,
            null,
            "this is a simple string."))
  }
}
