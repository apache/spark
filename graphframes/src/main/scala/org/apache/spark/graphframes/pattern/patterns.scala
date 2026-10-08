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

import scala.collection.mutable
import scala.util.parsing.combinator._

import org.apache.spark.graphframes.GraphFramesUnreachableException
import org.apache.spark.graphframes.InvalidParseException

/**
 * Parser for graph patterns for motif finding. Copied from GraphFrames with minor modification.
 */
private[graphframes] object PatternParser extends RegexParsers {
  private val vertexName: Parser[Vertex] = "[a-zA-Z0-9_]+".r ^^ { NamedVertex.apply }
  private val anonymousVertex: Parser[Vertex] = "" ^^ { _ => AnonymousVertex }
  private val vertex: Parser[Vertex] = "(" ~> (vertexName | anonymousVertex) <~ ")"
  private val namedEdge: Parser[Edge] =
    vertex ~ "-" ~ "[" ~ "[a-zA-Z0-9_]+".r ~ "]" ~ ("->" | "-") ~ vertex ^^ {
      case src ~ "-" ~ "[" ~ name ~ "]" ~ "->" ~ dst => NamedEdge(name, src, dst)
      case src ~ "-" ~ "[" ~ name ~ "]" ~ "-" ~ dst => UndirectedEdge(NamedEdge(name, src, dst))
      case _ => throw new GraphFramesUnreachableException()
    }
  val anonymousEdge: Parser[Edge] =
    vertex ~ "-" ~ "[" ~ "]" ~ ("->" | "-") ~ vertex ^^ {
      case src ~ "-" ~ "[" ~ "]" ~ "->" ~ dst => AnonymousEdge(src, dst)
      case src ~ "-" ~ "[" ~ "]" ~ "-" ~ dst => UndirectedEdge(AnonymousEdge(src, dst))
      case _ => throw new GraphFramesUnreachableException()
    }
  private val edge: Parser[Edge] = namedEdge | anonymousEdge
  private val negatedEdge: Parser[Pattern] =
    "!" ~ edge ^^ { case _ ~ e =>
      Negation(e)
    }
  private val pattern: Parser[Pattern] = edge | vertex | negatedEdge
  val patterns: Parser[List[Pattern]] = repsep(pattern, ";")
}

private[graphframes] object Pattern {
  def parse(s: String): Seq[Pattern] = {
    import PatternParser._
    val rewrittenStr: String = rewriteFixedLengthPattern(rewriteIncomingEdges(s))
    val result = parseAll(patterns, rewrittenStr) match {
      case result: Success[_] =>
        result.asInstanceOf[Success[Seq[Pattern]]].get
      case result: NoSuccess =>
        throw new InvalidParseException(
          s"Failed to parse bad motif string: '$s'.  Returned message: ${result.msg}")
    }
    assertValidPatterns(result)
    result
  }

  /**
   * Rewrite a motif string if there are incoming edges
   */
  private[graphframes] def rewriteIncomingEdges(patterns: String): String = {
    val reversedEdge =
      """(!?)\(([a-zA-Z0-9_]*)\)<-\[([a-zA-Z0-9_.*]*)\]-\(([a-zA-Z0-9_]*)\)""".r
    val bidirectionalEdge =
      """(!?)\(([a-zA-Z0-9_]*)\)<-\[([a-zA-Z0-9_.*]*)\]->\(([a-zA-Z0-9_]*)\)""".r

    val outgoingEdges: Seq[String] = patterns.split(";").toSeq.map { pattern =>
      pattern.trim match {
        case reversedEdge(negation, dst, edge, src) =>
          s"$negation($src)-[$edge]->($dst)"
        case bidirectionalEdge(negation, src, edge, dst) =>
          if (!negation.isEmpty) {
            throw new InvalidParseException(
              s"Motif finding does not support negated bidirectional edge: '$pattern'.")
          }
          if (edge.isEmpty || edge.contains("*")) {
            s"($src)-[$edge]->($dst);($dst)-[$edge]->($src)"
          } else {
            s"($src)-[${edge}1]->($dst);($dst)-[${edge}2]->($src)"
          }
        case original => original
      }
    }

    outgoingEdges.mkString(";")
  }

  /**
   * Rewrite fixed-length pattern
   */
  private[graphframes] def rewriteFixedLengthPattern(patterns: String): String = {
    val fixedLengthPattern =
      """(!?)\(([a-zA-Z0-9_]*)\)-\[([a-zA-Z0-9_]*)\*([0-9]+)\]->\(([a-zA-Z0-9_]*)\)""".r
    val expandedEdges: Seq[String] = patterns.split(";").toSeq.map { pattern =>
      pattern.trim match {
        case fixedLengthPattern(negation, src, name, num, dst) =>
          val hop: Int = num.toInt
          if (hop > 0) {
            val midVertices =
              if (src.isEmpty && dst.isEmpty) (1 until hop).map(i => s"__tmpv${i}")
              else (1 until hop).map(i => s"_${src}${dst}${i}")
            val vertices = src +: midVertices :+ dst
            vertices
              .sliding(2)
              .zipWithIndex
              .map {
                case (Seq(v1, v2), i) =>
                  if (name.isEmpty) s"${negation}(${v1})-[]->(${v2})"
                  else s"${negation}(${v1})-[_${name}${i + 1}]->(${v2})"
                case _ =>
                  throw new InvalidParseException(
                    s"Cannot rewrite fixed-length pattern as a chain: '$pattern'.")
              }
              .mkString(";")
          } else {
            throw new InvalidParseException(s"Hop must be greater than 0: '$pattern'.")
          }
        case original => original
      }
    }

    expandedEdges.mkString(";")
  }

  /**
   * Checks all Patterns for validity:
   *   - Disallow named edges within negated terms
   *   - Disallow term "()-[]->()" and its negation
   *   - Disallow name to be shared by a vertex and an edge
   * @throws InvalidParseException
   *   if an negated terms contain named edges
   */
  private def assertValidPatterns(patterns: Seq[Pattern]): Unit = {

    // vertexNames, edgeNames are used to check for duplicate names across vertices and edges
    val vertexNames = mutable.HashSet.empty[String]
    val edgeNames = mutable.HashSet.empty[String]
    def addVertex(v: Vertex): Unit = v match {
      case NamedVertex(name) =>
        if (edgeNames.contains(name)) {
          throw new InvalidParseException(
            s"Motif reused name '$name' for both a vertex and " +
              "an edge, which is not allowed.")
        }
        vertexNames += name
      case AnonymousVertex => // pass
    }
    def addEdge(e: Edge): Unit = e match {
      case NamedEdge(name, src, dst) =>
        if (vertexNames.contains(name)) {
          throw new InvalidParseException(
            s"Motif reused name '$name' for both a vertex and " +
              "an edge, which is not allowed.")
        }
        if (edgeNames.contains(name)) {
          throw new InvalidParseException(
            s"Motif reused name '$name' for multiple edges, " +
              "which is not allowed.")
        }
        edgeNames += name
        addVertex(src)
        addVertex(dst)
      case AnonymousEdge(src, dst) =>
        addVertex(src)
        addVertex(dst)
      case UndirectedEdge(edge) =>
        addEdge(edge)
    }

    patterns.foreach {
      case Negation(edge) =>
        edge match {
          case NamedEdge(name, src, dst) =>
            throw new InvalidParseException(
              "Motif finding does not support negated named " +
                s"edges, but the given pattern contained: !($src)-[$name]->($dst)")
          case AnonymousEdge(AnonymousVertex, AnonymousVertex) =>
            throw new InvalidParseException(
              "Motif finding does not support completely " +
                "anonymous negated edges !()-[]->().  Users can check for 0 edges in the graph " +
                "using the edges DataFrame.")
          case e @ UndirectedEdge(edge) =>
            edge match {
              case AnonymousEdge(AnonymousVertex, AnonymousVertex) =>
                throw new InvalidParseException(
                  "Motif finding does not support completely " +
                    "anonymous negated edges !()-[]-().  Users can check for the existence of " +
                    "edges in the graph using the edges DataFrame.")
              case _ => addEdge(e)
            }
          case e @ AnonymousEdge(_, _) =>
            addEdge(e)
        }
      case AnonymousEdge(AnonymousVertex, AnonymousVertex) =>
        throw new InvalidParseException(
          "Motif finding does not support completely " +
            "anonymous edges ()-[]->().  Users can check for the existence of edges in the " +
            "graph using the edges DataFrame.")
      case e @ UndirectedEdge(edge) =>
        edge match {
          case AnonymousEdge(AnonymousVertex, AnonymousVertex) =>
            throw new InvalidParseException(
              "Motif finding does not support completely " +
                "anonymous edges ()-[]-().  Users can check for the existence of edges in the " +
                "graph using the edges DataFrame.")
          case _ => addEdge(e)
        }
      case e @ AnonymousEdge(_, _) =>
        addEdge(e)
      case e @ NamedEdge(_, _, _) =>
        addEdge(e)
      case AnonymousVertex =>
        throw new InvalidParseException(
          "Motif finding does not allow a lone anonymous vertex " +
            "\"()\" in a motif.  Users can check for the existence of vertices in the graph " +
            "using the vertices DataFrame.")
      case v @ NamedVertex(_) =>
        addVertex(v)
    }
  }

  /**
   * Return the set of named vertices which only appear in negated terms, in sorted order.
   */
  private[graphframes] def findNamedVerticesOnlyInNegatedTerms(
      patterns: Seq[Pattern]): Seq[String] = {
    val vPos = findNamedElementsInOrder(
      patterns.filter(p => !p.isInstanceOf[Negation]),
      includeEdges = false).toSet
    val vNeg = findNamedElementsInOrder(
      patterns.filter(p => p.isInstanceOf[Negation]),
      includeEdges = false).toSet
    vNeg.diff(vPos).toSeq.sorted
  }

  /**
   * Return the set of named vertices (and optionally edges) appearing in the given patterns, in
   * the order they first appear in the sequence of patterns.
   * @param includeEdges
   *   If true, include named edges in the returned sequence.
   */
  private[graphframes] def findNamedElementsInOrder(
      patterns: Seq[Pattern],
      includeEdges: Boolean): Seq[String] = {
    val elementSet = mutable.LinkedHashSet.empty[String]
    def findNamedElementsHelper(pattern: Pattern): Unit = pattern match {
      case Negation(child) =>
        findNamedElementsHelper(child)
      case UndirectedEdge(child) =>
        findNamedElementsHelper(child)
        elementSet += "_pattern"
        elementSet += "_direction"
      case AnonymousVertex => // pass
      case NamedVertex(name) =>
        if (!elementSet.contains(name)) {
          elementSet += name
        }
      case AnonymousEdge(src, dst) =>
        findNamedElementsHelper(src)
        findNamedElementsHelper(dst)
      case NamedEdge(name, src, dst) =>
        findNamedElementsHelper(src)
        if (includeEdges && !elementSet.contains(name)) {
          elementSet += name
        }
        findNamedElementsHelper(dst)
    }
    patterns.foreach(findNamedElementsHelper)
    elementSet.toSeq
  }
}

private[graphframes] sealed trait Pattern

private[graphframes] case class Negation(child: Edge) extends Pattern

private[graphframes] sealed trait Vertex extends Pattern

private[graphframes] case object AnonymousVertex extends Vertex

private[graphframes] case class NamedVertex(name: String) extends Vertex

private[graphframes] sealed trait Edge extends Pattern

private[graphframes] case class UndirectedEdge(edge: Edge) extends Edge

private[graphframes] case class AnonymousEdge(src: Vertex, dst: Vertex) extends Edge

private[graphframes] case class NamedEdge(name: String, src: Vertex, dst: Vertex) extends Edge
