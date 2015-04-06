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

import scala.util.Random

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.rdd._
import org.apache.spark.graphx._

/** Implementation of SVD++ algorithm. */
object SVDPlusPlus {

  /** Configuration parameters for SVDPlusPlus. */
  class Conf(
      var rank: Int,
      var maxIters: Int,
      var minVal: Double,
      var maxVal: Double,
      var gamma1: Double,
      var gamma2: Double,
      var gamma6: Double,
      var gamma7: Double)
    extends Serializable

  /**
   * This method is now replaced by the updated version of `run()` and returns exactly
   * the same result.
   */
  @deprecated("Call run()", "1.4.0")
  def runSVDPlusPlus(edges: RDD[Edge[Double]], conf: Conf)
    : (Graph[(Array[Double], Array[Double], Double, Double), Double], Double) =
  {
    run(edges, conf)
  }

  /**
   * Implement SVD++ based on "Factorization Meets the Neighborhood:
   * a Multifaceted Collaborative Filtering Model",
   * available at [[http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf]].
   *
   * The prediction rule is rui = u + bu + bi + qi*(pu + |N(u)|^^-0.5^^*sum(y)),
   * see the details on page 6.
   *
   * @param edges edges for constructing the graph
   *
   * @param conf SVDPlusPlus parameters
   *
   * @return a graph with vertex attributes containing the trained model
   */
  def run(edges: RDD[Edge[Double]], conf: Conf)
    : (Graph[(Array[Double], Array[Double], Double, Double), Double], Double) =
  {
    // Generate default vertex attribute
    def defaultF(rank: Int): (Array[Double], Array[Double], Double, Double) = {
      // TODO: use a fixed random seed
      val v1 = Array.fill(rank)(Random.nextDouble())
      val v2 = Array.fill(rank)(Random.nextDouble())
      (v1, v2, 0.0, 0.0)
    }

    // calculate global rating mean
    edges.cache()
    val (rs, rc) = edges.map(e => (e.attr, 1L)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val u = rs / rc

    // construct graph
    var g = Graph.fromEdges(edges, defaultF(conf.rank)).cache()
    materialize(g)
    edges.unpersist()

    // Calculate initial bias and norm
    val t0 = g.aggregateMessages[(Long, Double)](
      ctx => { ctx.sendToSrc((1L, ctx.attr)); ctx.sendToDst((1L, ctx.attr)) },
      (g1, g2) => (g1._1 + g2._1, g1._2 + g2._2))

    val gJoinT0 = g.outerJoinVertices(t0) {
      (vid: VertexId, vd: (Array[Double], Array[Double], Double, Double),
       msg: Option[(Long, Double)]) =>
        (vd._1, vd._2, msg.get._2 / msg.get._1, 1.0 / scala.math.sqrt(msg.get._1))
    }.cache()
    materialize(gJoinT0)
    g.unpersist()
    g = gJoinT0

    def sendMsgTrainF(conf: Conf, u: Double)
        (ctx: EdgeContext[
          (Array[Double], Array[Double], Double, Double),
          Double,
          (Array[Double], Array[Double], Double)]) {
      val (usr, itm) = (ctx.srcAttr, ctx.dstAttr)
      val (p, q) = (usr._1, itm._1)
      val rank = p.length
      var pred = u + usr._3 + itm._3 + blas.ddot(rank, q, 1, usr._2, 1)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = ctx.attr - pred
      // updateP = (err * q - conf.gamma7 * p) * conf.gamma2
      val updateP = q.clone()
      blas.dscal(rank, err * conf.gamma2, updateP, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, p, 1, updateP, 1)
      // updateQ = (err * usr._2 - conf.gamma7 * q) * conf.gamma2
      val updateQ = usr._2.clone()
      blas.dscal(rank, err * conf.gamma2, updateQ, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, q, 1, updateQ, 1)
      // updateY = (err * usr._4 * q - conf.gamma7 * itm._2) * conf.gamma2
      val updateY = q.clone()
      blas.dscal(rank, err * usr._4 * conf.gamma2, updateY, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, itm._2, 1, updateY, 1)
      ctx.sendToSrc((updateP, updateY, (err - conf.gamma6 * usr._3) * conf.gamma1))
      ctx.sendToDst((updateQ, updateY, (err - conf.gamma6 * itm._3) * conf.gamma1))
    }

    for (i <- 0 until conf.maxIters) {
      // Phase 1, calculate pu + |N(u)|^(-0.5)*sum(y) for user nodes
      g.cache()
      val t1 = g.aggregateMessages[Array[Double]](
        ctx => ctx.sendToSrc(ctx.dstAttr._2),
        (g1, g2) => {
          val out = g1.clone()
          blas.daxpy(out.length, 1.0, g2, 1, out, 1)
          out
        })
      val gJoinT1 = g.outerJoinVertices(t1) {
        (vid: VertexId, vd: (Array[Double], Array[Double], Double, Double),
         msg: Option[Array[Double]]) =>
          if (msg.isDefined) {
            val out = vd._1.clone()
            blas.daxpy(out.length, vd._4, msg.get, 1, out, 1)
            (vd._1, out, vd._3, vd._4)
          } else {
            vd
          }
      }.cache()
      materialize(gJoinT1)
      g.unpersist()
      g = gJoinT1

      // Phase 2, update p for user nodes and q, y for item nodes
      g.cache()
      val t2 = g.aggregateMessages(
        sendMsgTrainF(conf, u),
        (g1: (Array[Double], Array[Double], Double), g2: (Array[Double], Array[Double], Double)) =>
        {
          val out1 = g1._1.clone()
          blas.daxpy(out1.length, 1.0, g2._1, 1, out1, 1)
          val out2 = g2._2.clone()
          blas.daxpy(out2.length, 1.0, g2._2, 1, out2, 1)
          (out1, out2, g1._3 + g2._3)
        })
      val gJoinT2 = g.outerJoinVertices(t2) {
        (vid: VertexId,
         vd: (Array[Double], Array[Double], Double, Double),
         msg: Option[(Array[Double], Array[Double], Double)]) => {
          val out1 = vd._1.clone()
          blas.daxpy(out1.length, 1.0, msg.get._1, 1, out1, 1)
          val out2 = vd._2.clone()
          blas.daxpy(out2.length, 1.0, msg.get._2, 1, out2, 1)
          (out1, out2, vd._3 + msg.get._3, vd._4)
        }
      }.cache()
      materialize(gJoinT2)
      g.unpersist()
      g = gJoinT2
    }

    // calculate error on training set
    def sendMsgTestF(conf: Conf, u: Double)
        (ctx: EdgeContext[(Array[Double], Array[Double], Double, Double), Double, Double]) {
      val (usr, itm) = (ctx.srcAttr, ctx.dstAttr)
      val (p, q) = (usr._1, itm._1)
      var pred = u + usr._3 + itm._3 + blas.ddot(q.length, q, 1, usr._2, 1)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = (ctx.attr - pred) * (ctx.attr - pred)
      ctx.sendToDst(err)
    }

    g.cache()
    val t3 = g.aggregateMessages[Double](sendMsgTestF(conf, u), _ + _)
    val gJoinT3 = g.outerJoinVertices(t3) {
      (vid: VertexId, vd: (Array[Double], Array[Double], Double, Double), msg: Option[Double]) =>
        if (msg.isDefined) (vd._1, vd._2, vd._3, msg.get) else vd
    }.cache()
    materialize(gJoinT3)
    g.unpersist()
    g = gJoinT3

    // Convert DoubleMatrix to Array[Double]:
    val newVertices = g.vertices.mapValues(v => (v._1.toArray, v._2.toArray, v._3, v._4))
    (Graph(newVertices, g.edges), u)
  }

  /**
   * Forces materialization of a Graph by count()ing its RDDs.
   */
  private def materialize(g: Graph[_,_]): Unit = {
    g.vertices.count()
    g.edges.count()
  }

}
