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

package org.apache.spark.graphframes.pattern

import org.apache.spark.graphframes.InvalidParseException
import org.apache.spark.graphframes.SparkFunSuite

class PatternSuite extends SparkFunSuite {

  test("good parses") {
    assert(Pattern.parse("(abc)") === Seq(NamedVertex("abc")))

    assert(
      Pattern.parse("(u)-[e]->(v)") ===
        Seq(NamedEdge("e", NamedVertex("u"), NamedVertex("v"))))

    assert(
      Pattern.parse("(u)-[e*1]->(v)") ===
        Seq(NamedEdge("_e1", NamedVertex("u"), NamedVertex("v"))))

    assert(
      Pattern.parse("(u)-[e*3]->(v)") ===
        Seq(
          NamedEdge("_e1", NamedVertex("u"), NamedVertex("_uv1")),
          NamedEdge("_e2", NamedVertex("_uv1"), NamedVertex("_uv2")),
          NamedEdge("_e3", NamedVertex("_uv2"), NamedVertex("v"))))

    assert(
      Pattern.parse("(u)-[e*3]->(v);(v)-[l*2]->(w);(w)-[k*1]->(p)") ===
        Seq(
          NamedEdge("_e1", NamedVertex("u"), NamedVertex("_uv1")),
          NamedEdge("_e2", NamedVertex("_uv1"), NamedVertex("_uv2")),
          NamedEdge("_e3", NamedVertex("_uv2"), NamedVertex("v")),
          NamedEdge("_l1", NamedVertex("v"), NamedVertex("_vw1")),
          NamedEdge("_l2", NamedVertex("_vw1"), NamedVertex("w")),
          NamedEdge("_k1", NamedVertex("w"), NamedVertex("p"))))

    assert(
      Pattern.parse("()-[]->(v)") ===
        Seq(AnonymousEdge(AnonymousVertex, NamedVertex("v"))))

    assert(
      Pattern.parse("()-[e]->()") ===
        Seq(NamedEdge("e", AnonymousVertex, AnonymousVertex)))

    assert(
      Pattern.parse("(u)-[e]->(u)") ===
        Seq(NamedEdge("e", NamedVertex("u"), NamedVertex("u"))))

    assert(
      Pattern.parse("(u); ()-[]->(v)") ===
        Seq(NamedVertex("u"), AnonymousEdge(AnonymousVertex, NamedVertex("v"))))

    assert(
      Pattern.parse("(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w)") ===
        Seq(
          AnonymousEdge(NamedVertex("u"), NamedVertex("v")),
          AnonymousEdge(NamedVertex("v"), NamedVertex("w")),
          Negation(AnonymousEdge(NamedVertex("u"), NamedVertex("w")))))

    assert(
      Pattern.parse("(u)-[*3]->(v)") ===
        Seq(
          AnonymousEdge(NamedVertex("u"), NamedVertex("_uv1")),
          AnonymousEdge(NamedVertex("_uv1"), NamedVertex("_uv2")),
          AnonymousEdge(NamedVertex("_uv2"), NamedVertex("v"))))

    assert(
      Pattern.parse("(u)-[*5]->(v)") ===
        Seq(
          AnonymousEdge(NamedVertex("u"), NamedVertex("_uv1")),
          AnonymousEdge(NamedVertex("_uv1"), NamedVertex("_uv2")),
          AnonymousEdge(NamedVertex("_uv2"), NamedVertex("_uv3")),
          AnonymousEdge(NamedVertex("_uv3"), NamedVertex("_uv4")),
          AnonymousEdge(NamedVertex("_uv4"), NamedVertex("v"))))

    assert(
      Pattern.parse("(u)-[*10]->(v)") ===
        Seq(
          AnonymousEdge(NamedVertex("u"), NamedVertex("_uv1")),
          AnonymousEdge(NamedVertex("_uv1"), NamedVertex("_uv2")),
          AnonymousEdge(NamedVertex("_uv2"), NamedVertex("_uv3")),
          AnonymousEdge(NamedVertex("_uv3"), NamedVertex("_uv4")),
          AnonymousEdge(NamedVertex("_uv4"), NamedVertex("_uv5")),
          AnonymousEdge(NamedVertex("_uv5"), NamedVertex("_uv6")),
          AnonymousEdge(NamedVertex("_uv6"), NamedVertex("_uv7")),
          AnonymousEdge(NamedVertex("_uv7"), NamedVertex("_uv8")),
          AnonymousEdge(NamedVertex("_uv8"), NamedVertex("_uv9")),
          AnonymousEdge(NamedVertex("_uv9"), NamedVertex("v"))))
  }

  test("good parses - undirected pattern") {
    assert(
      Pattern.parse("(u)-[e]-(v)") ===
        Seq(UndirectedEdge(NamedEdge("e", NamedVertex("u"), NamedVertex("v")))))

    assert(
      Pattern.parse("(u)-[e]-(v);(v)-[]-(k)") ===
        Seq(
          UndirectedEdge(NamedEdge("e", NamedVertex("u"), NamedVertex("v"))),
          UndirectedEdge(AnonymousEdge(NamedVertex("v"), NamedVertex("k")))))
  }

  test("rewrite incoming edges") {
    assert(Pattern.rewriteIncomingEdges("(u)<-[e]-(v);") === "(v)-[e]->(u)")
    assert(Pattern.rewriteIncomingEdges("!(u)<-[e]-(v);") === "!(v)-[e]->(u)")
    assert(
      Pattern.rewriteIncomingEdges("(u)<-[]-(v);(u)-[e]->(v)") === "(v)-[]->(u);(u)-[e]->(v)")
    assert(Pattern.rewriteIncomingEdges("(u)<-[]->(v)") === "(u)-[]->(v);(v)-[]->(u)")
    assert(Pattern.rewriteIncomingEdges("(u)<-[e]->(v)") === "(u)-[e1]->(v);(v)-[e2]->(u)")
    assert(Pattern.rewriteIncomingEdges("(u)<-[*5]-(v)") === "(v)-[*5]->(u)")
    assert(Pattern.rewriteIncomingEdges("(u)<-[*5]->(v)") === "(u)-[*5]->(v);(v)-[*5]->(u)")
    assert(
      Pattern.rewriteIncomingEdges(
        "(v1)<-[e*1..2]->(v2)") === "(v1)-[e*1..2]->(v2);(v2)-[e*1..2]->(v1)")
  }

  test("rewrite incoming edges and parse") {
    Pattern.parse("(v)<-[e]-(u)") === Pattern.parse("(u)-[e]->(v)")
    Pattern.parse("(v)<-[]-(u)") === Pattern.parse("(u)-[]->(v)")
    Pattern.parse("!(v)<-[]-(u)") === Pattern.parse("!(u)-[]->(v)")
    Pattern.parse("()<-[e]-()") === Pattern.parse("()-[e]->()")
    Pattern.parse("(u)-[]->(v); (w)<-[]-(v); !(w)<-[]-(u)") === Pattern.parse(
      "(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w)")
    Pattern.parse("(v)<-[*5]-(u)") === Pattern.parse("(u)-[*5]->(v)")
  }

  test("bad parses") {
    withClue("Failed to catch parse error with lone anonymous vertex") {
      intercept[InvalidParseException] {
        Pattern.parse("()")
      }
    }
    withClue("Failed to catch parse error with lone anonymous vertex") {
      intercept[InvalidParseException] {
        Pattern.parse("(a)-[]->(b); ()")
      }
    }
    withClue("Failed to catch parse error") {
      intercept[InvalidParseException] {
        Pattern.parse("(")
      }
    }
    withClue("Failed to catch parse error") {
      intercept[InvalidParseException] {
        Pattern.parse("->(a)")
      }
    }
    withClue("Failed to catch parse error with negated vertex") {
      intercept[InvalidParseException] {
        Pattern.parse("!(a)")
      }
    }
    withClue("Failed to catch parse error with negated named edge") {
      val msg = intercept[InvalidParseException] {
        Pattern.parse("!(a)-[ab]->(b)")
      }
      assert(msg.getMessage.contains("does not support negated named edges"))
    }
    withClue("Failed to catch parse error with negated named edge") {
      val msg = intercept[InvalidParseException] {
        Pattern.parse("!()-[ab]->()")
      }
      assert(msg.getMessage.contains("does not support negated named edges"))
    }
    withClue("Failed to catch parse error with double negative") {
      intercept[InvalidParseException] {
        Pattern.parse("!!(a)-[]->(b)")
      }
    }
    withClue("Failed to catch parse error with completely anonymous edge ()-[]->()") {
      intercept[InvalidParseException] {
        Pattern.parse("()-[]->()")
      }
    }
    withClue("Failed to catch parse error with completely anonymous negated edge !()-[]->()") {
      intercept[InvalidParseException] {
        Pattern.parse("!()-[]->()")
      }
    }
    withClue("Failed to catch parse error with completely anonymous undirected edge ()-[]-()") {
      intercept[InvalidParseException] {
        Pattern.parse("()-[]-()")
      }
    }
    withClue(
      "Failed to catch parse error with completely anonymous negated and undirected " +
        "edge !()-[]-()") {
      intercept[InvalidParseException] {
        Pattern.parse("!()-[]-()")
      }
    }
    withClue("Failed to catch parse error with reused element name") {
      intercept[InvalidParseException] {
        Pattern.parse("(a)-[]->(b); ()-[a]->()")
      }
    }
    withClue("Failed to catch parse error with reused element name") {
      intercept[InvalidParseException] {
        Pattern.parse("(a)-[a]->(b)")
      }
    }
    withClue("Failed to catch parse error with reused edge name") {
      intercept[InvalidParseException] {
        Pattern.parse("(a)-[e]->(b); ()-[e]->()")
      }
    }
    withClue("Failed to catch parse error with not support negated bidirectional edge") {
      intercept[InvalidParseException] {
        Pattern.parse("!(u)<-[]->(v)")
      }
    }
  }

  test("unsupported parse on the fixed length patterns") {
    withClue("Failed to catch parse error with graph frame unreachable") {
      intercept[InvalidParseException] {
        Pattern.parse("(u)-[*0]->(v)")
      }
    }

    withClue("Failed to catch parse error with bad motif string") {
      intercept[InvalidParseException] {
        Pattern.parse("(u)-[*]->(v)")
      }
    }
  }

  test("empty pattern should be parsable") {
    Pattern.parse("")
  }

  def testFindNamedVerticesOnlyInNegatedTerms(pattern: String, expected: Seq[String]): Unit = {
    test(s"findNamedVerticesOnlyInNegatedTerms: $pattern") {
      val patterns = Pattern.parse(pattern)
      val result = Pattern.findNamedVerticesOnlyInNegatedTerms(patterns)
      assert(result === expected)
    }
  }

  testFindNamedVerticesOnlyInNegatedTerms(
    "(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w)",
    Seq.empty[String])

  testFindNamedVerticesOnlyInNegatedTerms("(u)-[]->(v); (v)-[]->(w)", Seq.empty[String])

  testFindNamedVerticesOnlyInNegatedTerms("!(u)-[]->(v)", Seq("u", "v"))

  testFindNamedVerticesOnlyInNegatedTerms(
    "(u)-[]->(v); (v)-[]->(w); !(a)-[]->(b); !(v)-[]->(c)",
    Seq("a", "b", "c"))

  def testFindNamedElementsInOrder(pattern: String, expected: Seq[String]): Unit = {
    test(s"testFindNamedElementsInOrder: $pattern") {
      val patterns = Pattern.parse(pattern)
      val result = Pattern.findNamedElementsInOrder(patterns, includeEdges = true)
      assert(result === expected)
    }
  }

  testFindNamedElementsInOrder("(u)-[]->(v); (v)-[]->(w); !(u)-[]->(w)", Seq("u", "v", "w"))

  testFindNamedElementsInOrder("(u)-[]->(v); ()-[vw]->()", Seq("u", "v", "vw"))

  testFindNamedElementsInOrder(
    "(u)-[uv]->(v); (v)-[vw]->(w); !(u)-[]->(w); (x)",
    Seq("u", "uv", "v", "vw", "w", "x"))
}
