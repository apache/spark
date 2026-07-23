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

package org.apache.spark.sql.catalyst.rules

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class RuleIdCollectionSuite extends SparkFunSuite {

  test("rules needing ids should point to existing rules") {
    val rulesNeedingIds = {
      val field = RuleIdCollection.getClass.getDeclaredField("rulesNeedingIds")
      field.setAccessible(true)
      field.get(RuleIdCollection).asInstanceOf[Seq[String]]
    }

    val missingRules = rulesNeedingIds.filterNot { ruleName =>
      // Rule names come from `Rule.ruleName`, where singleton objects drop the trailing '$'.
      Seq(ruleName, s"${ruleName}$$").exists { className =>
        try {
          Utils.classForName(className)
          true
        } catch {
          case _: ClassNotFoundException => false
        }
      }
    }

    assert(
      missingRules.isEmpty,
      s"Found stale rule names in RuleIdCollection: ${missingRules.mkString(", ")}")
  }
}
