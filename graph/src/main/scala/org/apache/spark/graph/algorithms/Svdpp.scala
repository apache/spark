package org.apache.spark.graph.algorithms

import org.apache.spark._
import org.apache.spark.graph._
import scala.util.Random
import org.apache.commons.math.linear._

class VD (
  var v1: RealVector, // v1: p for user node, q for item node
  var v2: RealVector, // v2: pu + |N(u)|^(-0.5)*sum(y) for user node, y for item node
  var bias: Double,
  var norm: Double // only for user node
) extends Serializable

class Msg (
  var v1: RealVector,
  var v2: RealVector,
  var bias: Double
) extends Serializable

object Svdpp {
  // implement SVD++ based on http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf

  def run(sc: SparkContext, input: String): Graph[VD,Double] = {
    // defalut parameters
    val rank = 4
    val maxIters = 15
    val minVal = 0.0
    val maxVal = 5.0
    val gamma1 = 0.007
    val gamma2 = 0.007
    val gamma6 = 0.005
    val gamma7 = 0.015
	
    def defaultF(rank: Int) = {
      val p = new ArrayRealVector(rank)
      val w = new ArrayRealVector(rank)
      for (i <- 0 until rank) {
        p.setEntry(i, Random.nextDouble)
        w.setEntry(i, Random.nextDouble)
      }
      var vd = new VD(p, w, 0.0, 0.0)
      vd
    }

    // calculate initial norm and bias
    def mapF0(et: EdgeTriplet[VD, Double]): Iterator[(Vid, (Long, Double))] = {
      assert(et.srcAttr != null && et.dstAttr != null)
      Iterator((et.srcId, (1L, et.attr)), (et.dstId, (1L, et.attr)))
    }
    def reduceF0(g1: (Long, Double), g2: (Long, Double)) = {
      (g1._1 + g2._1, g1._2 + g2._2)
    }
    def updateF0(vid: Vid, vd: VD, msg: Option[(Long, Double)]) = {
      if (msg.isDefined) {
        vd.bias = msg.get._2 / msg.get._1
        vd.norm = 1.0 / scala.math.sqrt(msg.get._1)
      }
      vd
    }
	
    // read textfile
    val edges = sc.textFile(input).map { line => 
      val fields = line.split(",")
      Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
    }

    // calculate global rating mean
    val (rs, rc) = edges.map(e => (e.attr, 1L)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val u = rs / rc // global rating mean

    // make graph
    var g = Graph.fromEdges(edges, defaultF(rank), RandomVertexCut).cache()

    // calculate initial norm and bias
    val t0 = g.mapReduceTriplets(mapF0, reduceF0)
    g.outerJoinVertices(t0) {updateF0} 
	
    // phase 1
    def mapF1(et: EdgeTriplet[VD, Double]): Iterator[(Vid, RealVector)] = {
      assert(et.srcAttr != null && et.dstAttr != null)
      Iterator((et.srcId, et.dstAttr.v2)) // sum up y of connected item nodes
    }	
    def reduceF1(g1: RealVector, g2: RealVector) = {
      g1.add(g2)
    }
    def updateF1(vid: Vid, vd: VD, msg: Option[RealVector]) = {
      if (msg.isDefined) {
        vd.v2 = vd.v1.add(msg.get.mapMultiply(vd.norm)) // pu + |N(u)|^(-0.5)*sum(y)
      }
      vd
    }

    // phase 2
    def mapF2(et: EdgeTriplet[VD, Double]): Iterator[(Vid, Msg)] = {
      assert(et.srcAttr != null && et.dstAttr != null)
      val usr = et.srcAttr
      val itm = et.dstAttr
      var p = usr.v1
      var q = itm.v1
      val itmBias = 0.0
      val usrBias = 0.0
      var pred = u + usr.bias + itm.bias + q.dotProduct(usr.v2)
      if (pred < minVal) {
        pred = minVal
      }
      if (pred > maxVal) {
        pred = maxVal
      }
      val err = et.attr - pred
      val y = (q.mapMultiply(err*usr.norm)).subtract((usr.v2).mapMultiply(gamma7))
      val newP = (q.mapMultiply(err)).subtract(p.mapMultiply(gamma7)) // for each connected item q
      val newQ = (usr.v2.mapMultiply(err)).subtract(q.mapMultiply(gamma7))
      Iterator((et.srcId, new Msg(newP, y, err - gamma6*usr.bias)), (et.dstId, new Msg(newQ, y, err - gamma6*itm.bias)))
    }	
    def reduceF2(g1: Msg, g2: Msg):Msg = {
      g1.v1 = g1.v1.add(g2.v1)
      g1.v2 = g1.v2.add(g2.v2)
      g1.bias += g2.bias
      g1
    }
    def updateF2(vid: Vid, vd: VD, msg: Option[Msg]) = {
      if (msg.isDefined) {
        vd.v1 = vd.v1.add(msg.get.v1.mapMultiply(gamma2))
        if (vid % 2 == 1) { // item node update y
          vd.v2 = vd.v2.add(msg.get.v2.mapMultiply(gamma2))
        }
        vd.bias += msg.get.bias*gamma1
      }
      vd
    }

    for (i <- 0 until maxIters) {
      // phase 1
      val t1: VertexRDD[RealVector] = g.mapReduceTriplets(mapF1, reduceF1)
      g.outerJoinVertices(t1) {updateF1}  
      // phase 2
      val t2: VertexRDD[Msg] = g.mapReduceTriplets(mapF2, reduceF2)
      g.outerJoinVertices(t2) {updateF2}
    }
  g
  }
}
