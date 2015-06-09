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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.BeforeAndAfter

class AggregateSuite extends HiveComparisonTest with BeforeAndAfter {
  override def beforeAll() {
    TestHive.cacheTables = true
    TestHive.setConf(SQLConf.AGGREGATE_2, "true")
  }

  override def afterAll() {
    TestHive.cacheTables = false
    TestHive.setConf(SQLConf.AGGREGATE_2, "false")
  }

  createQueryTest("aggregation without group by expressions #1",
    """
      |SELECT
      |  count(value),
      |  max(key),
      |  min(key)
      |FROM src
    """.stripMargin, false)

  createQueryTest("aggregation without group by expressions #2",
    """
      |SELECT
      |  count(value),
      |  max(key),
      |  min(key),
      |  sum(key)
      |FROM src
    """.stripMargin, false)

  createQueryTest("aggregation without group by expressions #3",
    """
      |SELECT
      |  count(distinct value),
      |  max(key),
      |  min(key),
      |  sum(distinct key)
      |FROM src
    """.stripMargin, false)

  createQueryTest("aggregation without group by expressions #4",
    """
      |SELECT
      |  count(distinct value),
      |  max(key),
      |  min(key),
      |  sum(distinct key)
      |FROM src
    """.stripMargin, false)

  createQueryTest("aggregation without group by expressions #5",
    """
      |SELECT
      |  count(value) + 3,
      |  max(key) + 1,
      |  min(key) + 2,
      |  sum(key) + 5
      |FROM src
    """.stripMargin, false)

  createQueryTest("aggregation without group by expressions #6",
    """
      |SELECT
      |  count(distinct value) + 4,
      |  max(key) + 2,
      |  min(key) + 3,
      |  sum(distinct key) + 4
      |FROM src
    """.stripMargin, false)

  createQueryTest("aggregation with group by expressions #1",
    """
      |SELECT key + 3 as a, count(value), max(key), min(key)
      |FROM src group by key, value
      |ORDER BY a LIMIT 5
    """.stripMargin, false)

  createQueryTest("aggregation with group by expressions #2",
    """
      |SELECT
      |  key + 3 as a,
      |  count(value),
      |  max(key),
      |  min(key),
      |  sum(key)
      |FROM src
      |GROUP BY key, value
      |ORDER BY a LIMIT 5
    """.stripMargin, false)

  createQueryTest("aggregation with group by expressions #3",
    """
      |SELECT
      |  key + 3 as a,
      |  count(distinct value),
      |  max(key), min(key),
      |  sum(distinct key)
      |FROM src
      |GROUP BY key, value
      |ORDER BY a LIMIT 5
    """.stripMargin, false)

  createQueryTest("aggregation with group by expressions #5",
    """
      |SELECT
      |  (key + 3) * 2 as a,
      |  (key + 3) + count(distinct value),
      |  (key + 3) + max(key + 3),
      |  (key + 3) + min(key + 3),
      |  (key + 3) + sum(distinct (key + 3))
      |FROM src
      |GROUP BY key + 3, value
      |ORDER BY a LIMIT 5
    """.stripMargin, false)

  createQueryTest("aggregation with group by expressions #6",
    """
      |SELECT
      |  stddev_pop(key) as a,
      |  stddev_samp(key) as b
      |FROM src
      |GROUP BY key + 3, value
      |ORDER BY a, b LIMIT 5
    """.stripMargin, false)

// TODO currently the parser doesn't support the distinct
// in Hive UDAF
//  createQueryTest("aggregation with group by expressions #7",
//    """
//      |SELECT
//      |  stddev_pop(distinct key) as a,
//      |  stddev_samp(distinct key) as b
//      |FROM src
//      |GROUP BY key + 3, value
//      |ORDER BY a, b LIMIT 5
//    """.stripMargin, false)

  createQueryTest("aggregation with group by expressions #8",
    """
      |SELECT
      |  (key + 3) + count(distinct value, key) as a
      |FROM src
      |GROUP BY key + 3, value
      |ORDER BY a LIMIT 5
    """.stripMargin, false)
}
