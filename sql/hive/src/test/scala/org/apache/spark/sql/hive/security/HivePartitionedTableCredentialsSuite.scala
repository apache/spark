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

package org.apache.spark.sql.hive.security

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

object HivePartitionedTableCredentialsSuite extends QueryTest
  with SQLTestUtils with TestHiveSingleton {
  test("SPARK-36328: Reuse the FileSystem delegation token" +
    " while querying partitioned hive table.") {
    // The suite is based on the repro provided in SPARK-36328
    withTable("parttable") {
      // create partitioned table
      sql("create table parttable (key char(1), value int) partitioned by (p int);")
      sql("insert into table parttable partition(p=100) values ('d', 1), ('e', 2), ('f', 3);")
      sql("insert into table parttable partition(p=200) values ('d', 1), ('e', 2), ('f', 3);")
      sql("insert into table parttable partition(p=300) values ('d', 1), ('e', 2), ('f', 3);")
      // execute query
      checkAnswer(sql("select value, count(*) from parttable group by value."),
        Seq[Row](Row(1, 3), Row(2, 3), Row(3, 3)))
      // Only one token cached.
      assert(UserGroupInformation.getCurrentUser.getCredentials.getAllTokens.size() == 1)
    }
  }
}
