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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.plans.PlanTest

class CatalystQlSuite extends PlanTest {
  val parser = new CatalystQl()

  test("parse union/except/intersect") {
    parser.createPlan("select * from t1 union all select * from t2")
    parser.createPlan("select * from t1 union distinct select * from t2")
    parser.createPlan("select * from t1 union select * from t2")
    parser.createPlan("select * from t1 except select * from t2")
    parser.createPlan("select * from t1 intersect select * from t2")
    parser.createPlan("(select * from t1) union all (select * from t2)")
    parser.createPlan("(select * from t1) union distinct (select * from t2)")
    parser.createPlan("(select * from t1) union (select * from t2)")
    parser.createPlan("select * from ((select * from t1) union (select * from t2)) t")
  }

  test("window function: better support of parentheses") {
    parser.createPlan("select sum(product + 1) over (partition by ((1) + (product / 2)) " +
      "order by 2) from windowData")
    parser.createPlan("select sum(product + 1) over (partition by (1 + (product / 2)) " +
      "order by 2) from windowData")
    parser.createPlan("select sum(product + 1) over (partition by ((product / 2) + 1) " +
      "order by 2) from windowData")

    parser.createPlan("select sum(product + 1) over (partition by ((product) + (1)) order by 2) " +
      "from windowData")
    parser.createPlan("select sum(product + 1) over (partition by ((product) + 1) order by 2) " +
      "from windowData")
    parser.createPlan("select sum(product + 1) over (partition by (product + (1)) order by 2) " +
      "from windowData")
  }
}
