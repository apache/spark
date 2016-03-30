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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier

class TableIdentifierParserSuite extends SparkFunSuite {
  import CatalystSqlParser._

  test("table identifier") {
    // Regular names.
    assert(TableIdentifier("q") === parseTableIdentifier("q"))
    assert(TableIdentifier("q", Option("d")) === parseTableIdentifier("d.q"))

    // Illegal names.
    intercept[ParseException](parseTableIdentifier(""))
    intercept[ParseException](parseTableIdentifier("d.q.g"))

    // SQL Keywords.
    val keywords = Seq("select", "from", "where", "left", "right")
    keywords.foreach { keyword =>
      intercept[ParseException](parseTableIdentifier(keyword))
      assert(TableIdentifier(keyword) === parseTableIdentifier(s"`$keyword`"))
      assert(TableIdentifier(keyword, Option("db")) === parseTableIdentifier(s"db.`$keyword`"))
    }
  }
}
