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

package org.apache.spark.graphx.lib

import org.scalatest.FunSuite

import org.apache.spark.graphx._

class PatternMatchingSuite extends FunSuite with LocalSparkContext {
  test("Pattern Matching") {
    withSpark { sc =>
      val verts = sc.parallelize(List(
        (0L, "Alice"), (1L, "Acme"), (2L, "Bob"), (3L, "CompanyX"), (4L, "Cindy")))
      val edges = sc.parallelize(List(
        Edge(0L, 1L, "works_at"),
        Edge(2L, 1L, "works_at"),
        Edge(2L, 3L, "worked_at"),
        Edge(4L, 3L, "works_at")))
      val graph = Graph(verts, edges)

      val matchesByEndVertex = PatternMatching.run(graph, List(
        EdgePattern("works_at", false),
        EdgePattern("works_at", true),
        EdgePattern("worked_at", false),
        EdgePattern("works_at", true))).collect.toList

      assert(matchesByEndVertex === List((4L, List(
        Match(List(
          EdgeMatch(0L, 1L, "works_at"), // Alice works at Acme.
          EdgeMatch(2L, 1L, "works_at"), // Bob also works at Acme.
          EdgeMatch(2L, 3L, "worked_at"), // Bob formerly worked at CompanyX...
          EdgeMatch(4L, 3L, "works_at"))), // where Cindy currently works.
        Match(List(
          EdgeMatch(2L, 1L, "works_at"), // Alice works at Acme.
          EdgeMatch(2L, 1L, "works_at"), // Alice also works at Acme. Who would have guessed? (The
                                         // duplicate edge is an artifact of the PatternMatching
                                         // semantics.)
          EdgeMatch(2L, 3L, "worked_at"), // -- as before --
          EdgeMatch(4L, 3L, "works_at"))))))) // -- as before --

    }
  }
}
