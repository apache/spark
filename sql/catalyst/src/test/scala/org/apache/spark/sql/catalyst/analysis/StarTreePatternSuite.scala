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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * Pins the tree-pattern identity contract for the star-related expression nodes: each node carries
 * its own identity bit, so rules can prune on it.
 */
class StarTreePatternSuite extends SparkFunSuite {

  test("UnresolvedStar declares UNRESOLVED_STAR") {
    assert(UnresolvedStar(None).containsPattern(UNRESOLVED_STAR))
  }

  test("UnresolvedStarExceptOrReplace declares UNRESOLVED_STAR_EXCEPT_OR_REPLACE") {
    val node = UnresolvedStarExceptOrReplace(None, Seq.empty, None)
    assert(node.containsPattern(UNRESOLVED_STAR_EXCEPT_OR_REPLACE))
  }

  test("UnresolvedStarWithColumns declares UNRESOLVED_STAR_WITH_COLUMNS") {
    val node = UnresolvedStarWithColumns(Seq.empty, Seq.empty)
    assert(node.containsPattern(UNRESOLVED_STAR_WITH_COLUMNS))
  }

  test("UnresolvedStarWithColumnsRenames declares UNRESOLVED_STAR_WITH_COLUMNS_RENAMES") {
    val node = UnresolvedStarWithColumnsRenames(Seq.empty, Seq.empty)
    assert(node.containsPattern(UNRESOLVED_STAR_WITH_COLUMNS_RENAMES))
  }

  test("UnresolvedRegex declares UNRESOLVED_REGEX") {
    val node = UnresolvedRegex("col.*", None, caseSensitive = false)
    assert(node.containsPattern(UNRESOLVED_REGEX))
  }

  test("ResolvedStar declares RESOLVED_STAR") {
    assert(ResolvedStar(Seq.empty).containsPattern(RESOLVED_STAR))
  }

  test("UnresolvedDataFrameStar declares UNRESOLVED_DF_STAR") {
    assert(UnresolvedDataFrameStar(1L).containsPattern(UNRESOLVED_DF_STAR))
  }
}
