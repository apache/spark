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

package org.apache.spark.sql.sources

import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}


// please note that the META-INF/services had to be modified for the test directory for this to work
class DDLSourceLoadSuite extends DataSourceTest with SharedSQLContext {

  test("data sources with the same name") {
    intercept[RuntimeException] {
      spark.read.format("Fluet da Bomb").load()
    }
  }

  test("load data source from format alias") {
    spark.read.format("gathering quorum").load().schema ==
      StructType(Seq(StructField("stringType", StringType, nullable = false)))
  }

  test("specify full classname with duplicate formats") {
    spark.read.format("org.apache.spark.sql.sources.FakeSourceOne")
      .load().schema == StructType(Seq(StructField("stringType", StringType, nullable = false)))
  }

  test("should fail to load ORC without Hive Support") {
    val e = intercept[AnalysisException] {
      spark.read.format("orc").load()
    }
    assert(e.message.contains("The ORC data source must be used with Hive support enabled"))
  }
}


class FakeSourceOne extends RelationProvider with DataSourceRegister {

  def shortName(): String = "Fluet da Bomb"

  override def createRelation(cont: SQLContext, param: Map[String, String]): BaseRelation =
    new BaseRelation {
      override def sqlContext: SQLContext = cont

      override def schema: StructType =
        StructType(Seq(StructField("stringType", StringType, nullable = false)))
    }
}

class FakeSourceTwo extends RelationProvider  with DataSourceRegister {

  def shortName(): String = "Fluet da Bomb"

  override def createRelation(cont: SQLContext, param: Map[String, String]): BaseRelation =
    new BaseRelation {
      override def sqlContext: SQLContext = cont

      override def schema: StructType =
        StructType(Seq(StructField("stringType", StringType, nullable = false)))
    }
}

class FakeSourceThree extends RelationProvider with DataSourceRegister {

  def shortName(): String = "gathering quorum"

  override def createRelation(cont: SQLContext, param: Map[String, String]): BaseRelation =
    new BaseRelation {
      override def sqlContext: SQLContext = cont

      override def schema: StructType =
        StructType(Seq(StructField("stringType", StringType, nullable = false)))
    }
}
