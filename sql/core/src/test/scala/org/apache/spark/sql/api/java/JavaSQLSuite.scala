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
import org.apache.spark.sql.test.TestSQLContext

// Implicits
import scala.collection.JavaConversions._

class PersonBean extends Serializable {
  @BeanProperty
  var name: String = _

  @BeanProperty
  var age: Int = _
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
}
