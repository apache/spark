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

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/** Pattern matching of paths in graphs. */
object PatternMatching {

  /**
   * Searches for the specified pattern across all paths in the graph. Returns matching paths
   * organized by their terminal vertices. Note that an edge may be traversed more than once in the
   * same path; the caller must filter out such matches separately if this is undesired.
   *
   * Patterns are expressed as sequences of EdgePattern objects. These are applied in order, giving
   * the same effect as if the algorithm examined every path in the graph of the same length as
   * `pattern` and matched the path's edges in order against the EdgePattern objects.
   */
  def run[VD, ED: ClassTag](
      graph: Graph[VD, ED], pattern: List[EdgePattern[ED]]): VertexRDD[List[Match[ED]]] = {
    val startMatches = graph.mapVertices((vid, attr) => Set.empty[PartialMatch[ED]])

    val emptyPartialMatch = new PartialMatch(List.empty, pattern)
    val partialMatches = Pregel[Set[PartialMatch[ED]], ED, Set[PartialMatch[ED]]](
      startMatches, Set(emptyPartialMatch))(
      (vid, oldMatches, newMatches) => newMatches,
      et => {
        val msgsForDst = et.srcAttr.flatMap(_.tryMatch(et.srcId, et)).diff(et.dstAttr)
        val msgsForSrc = et.dstAttr.flatMap(_.tryMatch(et.dstId, et)).diff(et.srcAttr)
        if (msgsForSrc.nonEmpty && msgsForDst.nonEmpty) {
          Iterator((et.srcId, msgsForSrc), (et.dstId, msgsForDst))
        } else if (msgsForSrc.nonEmpty) {
          Iterator((et.srcId, msgsForSrc))
        } else if (msgsForDst.nonEmpty) {
          Iterator((et.dstId, msgsForDst))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a ++ b)

    partialMatches.vertices.mapValues(_.filter(_.isComplete).map(_.toCompleteMatch).toList)
      .filter(_._2.nonEmpty)
  }
}

/**
 * Within a pattern, matches an edge with the specified attribute. If `matchDstFirst` is `false`
 * (the default), the source vertex must be encountered first in the path traversal; otherwise the
 * destination vertex must be encountered first.
 */
case class EdgePattern[ED](attr: ED, matchDstFirst: Boolean = false) extends Serializable {
  /** Matches an edge against the EdgePattern, where we are currently traversing curVertex. */
  def matches(curVertex: VertexId, et: EdgeTriplet[_, ED]): Boolean = {
    val attrMatches = (attr == et.attr)
    val directionMatches = if (matchDstFirst) curVertex == et.dstId else curVertex == et.srcId
    attrMatches && directionMatches
  }
}

/** Within a successful match, represents a single matched edge. */
case class EdgeMatch[ED](srcId: VertexId, dstId: VertexId, attr: ED) extends Serializable {
  override def toString = s"$srcId --[$attr]--> $dstId"
}

/** Represents a successful match composed of a sequence of matching edges. */
case class Match[ED](path: List[EdgeMatch[ED]]) extends Serializable

private case class PartialMatch[ED](
    matched: List[EdgeMatch[ED]], remaining: List[EdgePattern[ED]]) extends Serializable {
  /**
   * Attempts to match the given current vertex and edge triplet, returning an augmented
   * PartialMatch if successful.
   */
  def tryMatch(curVertex: VertexId, et: EdgeTriplet[_, ED]): Option[PartialMatch[ED]] = {
    remaining match {
      case pattern :: rest if pattern.matches(curVertex, et) =>
        val newMatch = EdgeMatch(et.srcId, et.dstId, pattern.attr)
        Some(PartialMatch(newMatch :: matched, rest))
      case _ => None
    }
  }

  def isComplete: Boolean = remaining.isEmpty

  def toCompleteMatch: Match[ED] = {
    assert(isComplete)
    new Match(matched.reverse)
  }
}
