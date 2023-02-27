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

import java.util.concurrent.atomic.AtomicLong

import io.grpc.inprocess.InProcessChannelBuilder
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.client.util.ConnectFunSuite

class SQLImplicitsTestSuite extends ConnectFunSuite with BeforeAndAfterAll {
  private var session: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val client = SparkConnectClient(
      proto.UserContext.newBuilder().build(),
      InProcessChannelBuilder.forName("/dev/null").build())
    session =
      new SparkSession(client, cleaner = SparkSession.cleaner, planIdGenerator = new AtomicLong)
  }

  test("column resolution") {
    val spark = session
    import spark.implicits._
    def assertEqual(left: Column, right: Column): Unit = assert(left == right)
    assertEqual($"x", Column("x"))
    assertEqual('y, Column("y"))
  }
}
