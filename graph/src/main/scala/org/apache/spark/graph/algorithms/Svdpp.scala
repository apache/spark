package org.apache.spark.graph.algorithms

import org.apache.spark.rdd._
import org.apache.spark.graph._
import scala.util.Random
import org.apache.commons.math.linear._

class SvdppConf( // Svdpp parameters
  var rank: Int,
  var maxIters: Int,
  var minVal: Double,
  var maxVal: Double,
  var gamma1: Double,
  var gamma2: Double,
  var gamma6: Double,
  var gamma7: Double) extends Serializable

object Svdpp {
  /**
   * Implement SVD++ based on "Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering Model",
   * paper is available at [[http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf]].
   * The prediction rule is rui = u + bu + bi + qi*(pu + |N(u)|^(-0.5)*sum(y)), see the details on page 6.
   *
   * @param edges edges for constructing the graph
   *
   * @param conf Svdpp parameters
   *
   * @return a graph with vertex attributes containing the trained model
   */

  def run(edges: RDD[Edge[Double]], conf: SvdppConf): (Graph[(RealVector, RealVector, Double, Double), Double], Double) = {

    // generate default vertex attribute
    def defaultF(rank: Int): (RealVector, RealVector, Double, Double) = {
      val v1 = new ArrayRealVector(rank)
      val v2 = new ArrayRealVector(rank)
      for (i <- 0 until rank) {
        v1.setEntry(i, Random.nextDouble)
        v2.setEntry(i, Random.nextDouble)
      }
      (v1, v2, 0.0, 0.0)
    }

    // calculate global rating mean
    val (rs, rc) = edges.map(e => (e.attr, 1L)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val u = rs / rc

    // construct graph
    var g = Graph.fromEdges(edges, defaultF(conf.rank)).cache()

    // calculate initial bias and norm
    var t0 = g.mapReduceTriplets(et =>
      Iterator((et.srcId, (1L, et.attr)), (et.dstId, (1L, et.attr))), (g1: (Long, Double), g2: (Long, Double)) => (g1._1 + g2._1, g1._2 + g2._2))
    g = g.outerJoinVertices(t0) { (vid: Vid, vd: (RealVector, RealVector, Double, Double), msg: Option[(Long, Double)]) =>
      (vd._1, vd._2, msg.get._2 / msg.get._1, 1.0 / scala.math.sqrt(msg.get._1))
    }

    def mapTrainF(conf: SvdppConf, u: Double)(et: EdgeTriplet[(RealVector, RealVector, Double, Double), Double])
      : Iterator[(Vid, (RealVector, RealVector, Double))] = {
      val (usr, itm) = (et.srcAttr, et.dstAttr)
      val (p, q) = (usr._1, itm._1)
      var pred = u + usr._3 + itm._3 + q.dotProduct(usr._2)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = et.attr - pred
      val updateP = ((q.mapMultiply(err)).subtract(p.mapMultiply(conf.gamma7))).mapMultiply(conf.gamma2)
      val updateQ = ((usr._2.mapMultiply(err)).subtract(q.mapMultiply(conf.gamma7))).mapMultiply(conf.gamma2)
      val updateY = ((q.mapMultiply(err * usr._4)).subtract((itm._2).mapMultiply(conf.gamma7))).mapMultiply(conf.gamma2)
      Iterator((et.srcId, (updateP, updateY, (err - conf.gamma6 * usr._3) * conf.gamma1)),
        (et.dstId, (updateQ, updateY, (err - conf.gamma6 * itm._3) * conf.gamma1)))
    }

    for (i <- 0 until conf.maxIters) {
      // phase 1, calculate pu + |N(u)|^(-0.5)*sum(y) for user nodes
      var t1 = g.mapReduceTriplets(et => Iterator((et.srcId, et.dstAttr._2)), (g1: RealVector, g2: RealVector) => g1.add(g2))
      g = g.outerJoinVertices(t1) { (vid: Vid, vd: (RealVector, RealVector, Double, Double), msg: Option[RealVector]) =>
        if (msg.isDefined) (vd._1, vd._1.add(msg.get.mapMultiply(vd._4)), vd._3, vd._4) else vd
      }
      // phase 2, update p for user nodes and q, y for item nodes
      val t2 = g.mapReduceTriplets(mapTrainF(conf, u), (g1: (RealVector, RealVector, Double), g2: (RealVector, RealVector, Double)) =>
        (g1._1.add(g2._1), g1._2.add(g2._2), g1._3 + g2._3))
      g = g.outerJoinVertices(t2) { (vid: Vid, vd: (RealVector, RealVector, Double, Double), msg: Option[(RealVector, RealVector, Double)]) =>
        (vd._1.add(msg.get._1), vd._2.add(msg.get._2), vd._3 + msg.get._3, vd._4)
      }
    }

    // calculate error on training set
    def mapTestF(conf: SvdppConf, u: Double)(et: EdgeTriplet[(RealVector, RealVector, Double, Double), Double]): Iterator[(Vid, Double)] = {
      val (usr, itm) = (et.srcAttr, et.dstAttr)
      val (p, q) = (usr._1, itm._1)
      var pred = u + usr._3 + itm._3 + q.dotProduct(usr._2)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = (et.attr - pred) * (et.attr - pred)
      Iterator((et.dstId, err))
    }
    val t3 = g.mapReduceTriplets(mapTestF(conf, u), (g1: Double, g2: Double) => g1 + g2)
    g = g.outerJoinVertices(t3) { (vid: Vid, vd: (RealVector, RealVector, Double, Double), msg: Option[Double]) =>
      if (msg.isDefined) (vd._1, vd._2, vd._3, msg.get) else vd
    }
    (g, u)
  }
}
