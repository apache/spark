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

package org.apache.spark.sql.connect.planner

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.types.{DataType, StructType}

class SparkConnectWithSessionExtensionSuite extends SparkFunSuite {

  case class MyParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {
    override def parsePlan(sqlText: String): LogicalPlan =
      delegate.parsePlan(sqlText)

    override def parseExpression(sqlText: String): Expression =
      delegate.parseExpression(sqlText)

    override def parseTableIdentifier(sqlText: String): TableIdentifier =
      delegate.parseTableIdentifier(sqlText)

    override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
      delegate.parseFunctionIdentifier(sqlText)

    override def parseMultipartIdentifier(sqlText: String): Seq[String] =
      delegate.parseMultipartIdentifier(sqlText) :+ "FROM_MY_PARSER"

    override def parseTableSchema(sqlText: String): StructType =
      delegate.parseTableSchema(sqlText)

    override def parseDataType(sqlText: String): DataType =
      delegate.parseDataType(sqlText)

    override def parseQuery(sqlText: String): LogicalPlan =
      delegate.parseQuery(sqlText)

    override def parseRoutineParam(sqlText: String): StructType =
      delegate.parseRoutineParam(sqlText)
  }

  test("Parse table name with test parser") {
    val spark = classic.SparkSession
      .builder()
      .master("local[1]")
      .withExtensions(extension => extension.injectParser(MyParser))
      .getOrCreate()

    val read = proto.Read.newBuilder().build()
    val readWithTable = read.toBuilder
      .setNamedTable(proto.Read.NamedTable.newBuilder.setUnparsedIdentifier("name").build())
      .build()
    val rel = proto.Relation.newBuilder.setRead(readWithTable).build()

    val res = new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(spark))
      .transformRelation(rel)

    assert(res !== null)
    assert(res.nodeName === "UnresolvedRelation")
    assert(
      res.asInstanceOf[UnresolvedRelation].multipartIdentifier ===
        Seq("name", "FROM_MY_PARSER"))

    spark.stop()
  }
}
