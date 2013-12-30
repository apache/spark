package org.apache.spark.graph.algorithms

import org.apache.spark.rdd._
import org.apache.spark.graph._
import scala.util.Random
import org.apache.commons.math.linear._

class VT( // vertex type
  var v1: RealVector, // v1: p for user node, q for item node
  var v2: RealVector, // v2: pu + |N(u)|^(-0.5)*sum(y) for user node, y for item node
  var bias: Double,
  var norm: Double // |N(u)|^(-0.5) for user node
  ) extends Serializable

class Msg( // message
  var v1: RealVector,
  var v2: RealVector,
  var bias: Double) extends Serializable

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

  def run(edges: RDD[Edge[Double]], conf: SvdppConf): Graph[VT, Double] = {

    // generate default vertex attribute
    def defaultF(rank: Int) = {
      val v1 = new ArrayRealVector(rank)
      val v2 = new ArrayRealVector(rank)
      for (i <- 0 until rank) {
        v1.setEntry(i, Random.nextDouble)
        v2.setEntry(i, Random.nextDouble)
      }
      var vd = new VT(v1, v2, 0.0, 0.0)
      vd
    }

    // calculate global rating mean
    val (rs, rc) = edges.map(e => (e.attr, 1L)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val u = rs / rc // global rating mean

    // construct graph
    var g = Graph.fromEdges(edges, defaultF(conf.rank)).cache()

    // calculate initial bias and norm
    var t0: VertexRDD[(Long, Double)] = g.mapReduceTriplets(et =>
      Iterator((et.srcId, (1L, et.attr)), (et.dstId, (1L, et.attr))),
      (g1: (Long, Double), g2: (Long, Double)) =>
      (g1._1 + g2._1, g1._2 + g2._2))
    g = g.outerJoinVertices(t0) {
      (vid: Vid, vd: VT, msg: Option[(Long, Double)]) =>
        vd.bias = msg.get._2 / msg.get._1; vd.norm = 1.0 / scala.math.sqrt(msg.get._1)
        vd
    }

    def mapTrainF(conf: SvdppConf, u: Double)(et: EdgeTriplet[VT, Double]): Iterator[(Vid, Msg)] = {
      assert(et.srcAttr != null && et.dstAttr != null)
      val (usr, itm) = (et.srcAttr, et.dstAttr)
      val (p, q) = (usr.v1, itm.v1)
      var pred = u + usr.bias + itm.bias + q.dotProduct(usr.v2)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = et.attr - pred
      val updateP = ((q.mapMultiply(err)).subtract(p.mapMultiply(conf.gamma7))).mapMultiply(conf.gamma2)
      val updateQ = ((usr.v2.mapMultiply(err)).subtract(q.mapMultiply(conf.gamma7))).mapMultiply(conf.gamma2)
      val updateY = ((q.mapMultiply(err * usr.norm)).subtract((itm.v2).mapMultiply(conf.gamma7))).mapMultiply(conf.gamma2)
      Iterator((et.srcId, new Msg(updateP, updateY, (err - conf.gamma6 * usr.bias) * conf.gamma1)),
        (et.dstId, new Msg(updateQ, updateY, (err - conf.gamma6 * itm.bias) * conf.gamma1)))
    }

    for (i <- 0 until conf.maxIters) {
      // phase 1, calculate v2 for user nodes
      var t1 = g.mapReduceTriplets(et =>
        Iterator((et.srcId, et.dstAttr.v2)),
        (g1: RealVector, g2: RealVector) => g1.add(g2))
      g = g.outerJoinVertices(t1) { (vid: Vid, vd: VT, msg: Option[RealVector]) =>
        if (msg.isDefined) vd.v2 = vd.v1.add(msg.get.mapMultiply(vd.norm))
        vd
      }
      // phase 2, update p for user nodes and q, y for item nodes
      val t2: VertexRDD[Msg] = g.mapReduceTriplets(mapTrainF(conf, u), (g1: Msg, g2: Msg) => {
        g1.v1 = g1.v1.add(g2.v1)
        g1.v2 = g1.v2.add(g2.v2)
        g1.bias += g2.bias
        g1
      })
      g = g.outerJoinVertices(t2) { (vid: Vid, vd: VT, msg: Option[Msg]) =>
        vd.v1 = vd.v1.add(msg.get.v1)
        if (vid % 2 == 1) vd.v2 = vd.v2.add(msg.get.v2)
        vd.bias += msg.get.bias
        vd
      }
    }

    // calculate error on training set
    def mapTestF(conf: SvdppConf, u: Double)(et: EdgeTriplet[VT, Double]): Iterator[(Vid, Double)] = {
      assert(et.srcAttr != null && et.dstAttr != null)
      val (usr, itm) = (et.srcAttr, et.dstAttr)
      val (p, q) = (usr.v1, itm.v1)
      var pred = u + usr.bias + itm.bias + q.dotProduct(usr.v2)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = (et.attr - pred) * (et.attr - pred)
      Iterator((et.dstId, err))
    }
    val t3: VertexRDD[Double] = g.mapReduceTriplets(mapTestF(conf, u), _ + _)
    g.outerJoinVertices(t3) { (vid: Vid, vd: VT, msg: Option[Double]) =>
      if (msg.isDefined && vid % 2 == 1) vd.norm = msg.get // item nodes sum up the errors
      vd
    }
    g
  }
}
