package spark.graph

import spark._
import spark.SparkContext._

// import breeze.linalg._

object Analytics {


  /**
   * Compute the PageRank of a graph returning the pagerank of each vertex as an RDD
   */
  def pagerank[VD: Manifest, ED: Manifest](graph: Graph[VD, ED], numIter: Int) = {
    // Compute the out degree of each vertex
    val pagerankGraph = graph.updateVertices[Int, (Int, Float)](graph.outDegrees,
      (vertex, deg) => (deg.getOrElse(0), 1.0F)
    )
    GraphLab.iterateGA[(Int, Float), ED, Float](pagerankGraph)(
      (me_id, edge) => edge.src.data._2 / edge.src.data._1, // gather
      (a: Float, b: Float) => a + b, // merge
      (vertex, a: Option[Float]) => (vertex.data._1, (0.15F + 0.85F * a.getOrElse(0F))), // apply
      numIter).mapVertices{ case Vertex(id, (outDeg, r)) => Vertex(id, r) }
  }


  /**
   * Compute the PageRank of a graph returning the pagerank of each vertex as an RDD
   */
  def pregelPagerank[VD: Manifest, ED: Manifest](graph: Graph[VD, ED], numIter: Int) = {
    // Compute the out degree of each vertex
    val pagerankGraph = graph.updateVertices[Int, (Int, Float)](graph.outDegrees,
      (vertex, deg) => (deg.getOrElse(0), 1.0F)
    )
    Pregel.iterate[(Int, Float), ED, Float](pagerankGraph)(
      (vertex, a: Float) => (vertex.data._1, (0.15F + 0.85F * a)), // apply
      (me_id, edge) => Some(edge.src.data._2 / edge.src.data._1), // gather
      (a: Float, b: Float) => a + b, // merge
      1.0F,
      numIter).mapVertices{ case Vertex(id, (outDeg, r)) => Vertex(id, r) }
  }


  /**
   * Compute the PageRank of a graph returning the pagerank of each vertex as an RDD
   */
  def dynamicPagerank[VD: Manifest, ED: Manifest](graph: Graph[VD, ED],
    tol: Float, maxIter: Int = 10) = {
    // Compute the out degree of each vertex
    val pagerankGraph = graph.updateVertices[Int, (Int, Float, Float)](graph.outDegrees,
      (vertex, degIter) => (degIter.sum, 1.0F, 1.0F)
    )

    // Run PageRank
    GraphLab.iterateGAS(pagerankGraph)(
      (me_id, edge) => edge.src.data._2 / edge.src.data._1, // gather
      (a: Float, b: Float) => a + b,
      (vertex, a: Option[Float]) =>
        (vertex.data._1, (0.15F + 0.85F * a.getOrElse(0F)), vertex.data._2), // apply
      (me_id, edge) => math.abs(edge.src.data._2 - edge.dst.data._1) > tol, // scatter
      maxIter).mapVertices { case Vertex(vid, data) => Vertex(vid, data._2) }
  }



  /**
   * Compute the connected component membership of each vertex
   * and return an RDD with the vertex value containing the
   * lowest vertex id in the connected component containing
   * that vertex.
   */
  def connectedComponents[VD: Manifest, ED: Manifest](graph: Graph[VD, ED], numIter: Int) = {
    val ccGraph = graph.mapVertices { case Vertex(vid, _) => Vertex(vid, vid) }
    GraphLab.iterateGA[Int, ED, Int](ccGraph)(
      (me_id, edge) => edge.otherVertex(me_id).data, // gather
      (a: Int, b: Int) => math.min(a, b), // merge
      (v, a: Option[Int]) => math.min(v.data, a.getOrElse(Integer.MAX_VALUE)), // apply
      numIter,
      gatherDirection = EdgeDirection.Both)
  }


  /**
   * Compute the shortest path to a set of markers
   */
  def shortestPath[VD: Manifest](graph: Graph[VD, Float], sources: List[Int], numIter: Int) = {
    val sourceSet = sources.toSet
    val spGraph = graph.mapVertices {
      case Vertex(vid, _) => Vertex(vid, (if(sourceSet.contains(vid)) 0.0F else Float.MaxValue))
    }
    GraphLab.iterateGA[Float, Float, Float](spGraph)(
      (me_id, edge) => edge.otherVertex(me_id).data + edge.data, // gather
      (a: Float, b: Float) => math.min(a, b), // merge
      (v, a: Option[Float]) => math.min(v.data, a.getOrElse(Float.MaxValue)), // apply
      numIter,
      gatherDirection = EdgeDirection.In)
  }

  // /**
  //  * Compute the connected component membership of each vertex
  //  * and return an RDD with the vertex value containing the
  //  * lowest vertex id in the connected component containing
  //  * that vertex.
  //  */
  // def dynamicConnectedComponents[VD: Manifest, ED: Manifest](graph: Graph[VD, ED],
  //   numIter: Int = Int.MaxValue) = {

  //   val vertices = graph.vertices.mapPartitions(iter => iter.map { case (vid, _) => (vid, vid) })
  //   val edges = graph.edges // .mapValues(v => None)
  //   val ccGraph = new Graph(vertices, edges)

  //   ccGraph.iterateDynamic(
  //     (me_id, edge) => edge.otherVertex(me_id).data, // gather
  //     (a: Int, b: Int) => math.min(a, b), // merge
  //     Integer.MAX_VALUE,
  //     (v, a: Int) => math.min(v.data, a), // apply
  //     (me_id, edge) => edge.otherVertex(me_id).data > edge.vertex(me_id).data, // scatter
  //     numIter,
  //     gatherEdges = EdgeDirection.Both,
  //     scatterEdges = EdgeDirection.Both).vertices
  //   //
  //   //    graph_ret.vertices.collect.foreach(println)
  //   //    graph_ret.edges.take(10).foreach(println)
  // }




  // /**
  //  * Compute the shortest path to a set of markers
  //  */
  //  def dynamicShortestPath[VD: Manifest, ED: Manifest](graph: Graph[VD, Float],
  //   sources: List[Int], numIter: Int) = {
  //   val sourceSet = sources.toSet
  //   val vertices = graph.vertices.mapPartitions(
  //     iter => iter.map {
  //       case (vid, _) => (vid, (if(sourceSet.contains(vid)) 0.0F else Float.MaxValue) )
  //       });

  //   val edges = graph.edges // .mapValues(v => None)
  //   val spGraph = new Graph(vertices, edges)

  //   val niterations = Int.MaxValue
  //   spGraph.iterateDynamic(
  //     (me_id, edge) => edge.otherVertex(me_id).data + edge.data, // gather
  //     (a: Float, b: Float) => math.min(a, b), // merge
  //     Float.MaxValue,
  //     (v, a: Float) => math.min(v.data, a), // apply
  //     (me_id, edge) => edge.vertex(me_id).data + edge.data < edge.otherVertex(me_id).data, // scatter
  //     numIter,
  //     gatherEdges = EdgeDirection.In,
  //     scatterEdges = EdgeDirection.Out).vertices
  // }




  // /**
  //  *
  //  */
  // def alternatingLeastSquares[VD: ClassManifest, ED: ClassManifest](graph: Graph[VD, Double],
  //   latentK: Int, lambda: Double, numIter: Int) = {
  //   val vertices = graph.vertices.mapPartitions( _.map {
  //       case (vid, _) => (vid,  Array.fill(latentK){ scala.util.Random.nextDouble() } )
  //       }).cache
  //   val maxUser = graph.edges.map(_._1).reduce(math.max(_,_))
  //   val edges = graph.edges // .mapValues(v => None)
  //   val alsGraph = new Graph(vertices, edges)
  //   alsGraph.numVPart = graph.numVPart
  //   alsGraph.numEPart = graph.numEPart

  //   val niterations = Int.MaxValue
  //   alsGraph.iterateDynamic[(Array[Double], Array[Double])](
  //     (me_id, edge) => { // gather
  //       val X = edge.otherVertex(me_id).data
  //       val y = edge.data
  //       val Xy = X.map(_ * y)
  //       val XtX = (for(i <- 0 until latentK; j <- i until latentK) yield(X(i) * X(j))).toArray
  //       (Xy, XtX)
  //     },
  //     (a, b) => {
  //     // The difference between the while loop and the zip is a FACTOR OF TWO in overall
  //     //  runtime
  //       var i = 0
  //       while(i < a._1.length) { a._1(i) += b._1(i); i += 1 }
  //       i = 0
  //       while(i < a._2.length) { a._2(i) += b._2(i); i += 1 }
  //       a
  //       // (a._1.zip(b._1).map{ case (q,r) => q+r }, a._2.zip(b._2).map{ case (q,r) => q+r })
  //     },
  //     (Array.empty[Double], Array.empty[Double]), // default value is empty
  //     (vertex, accum) => { // apply
  //       val XyArray  = accum._1
  //       val XtXArray = accum._2
  //       if(XyArray.isEmpty) vertex.data // no neighbors
  //       else {
  //         val XtX = DenseMatrix.tabulate(latentK,latentK){ (i,j) =>
  //           (if(i < j) XtXArray(i + (j+1)*j/2) else XtXArray(i + (j+1)*j/2)) +
  //           (if(i == j) lambda else 1.0F) //regularization
  //         }
  //         val Xy = DenseMatrix.create(latentK,1,XyArray)
  //         val w = XtX \ Xy
  //         w.data
  //       }
  //     },
  //     (me_id, edge) => true,
  //     numIter,
  //     gatherEdges = EdgeDirection.Both,
  //     scatterEdges = EdgeDirection.Both,
  //     vertex => vertex.id < maxUser).vertices
  // }



  def main(args: Array[String]) = {
    val host = args(0)
    val taskType = args(1)
    val fname = args(2)
    val options =  args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    // System.setProperty("spark.serializer", "spark.KryoSerializer")
    // //System.setProperty("spark.shuffle.compress", "false")
    // System.setProperty("spark.kryo.registrator", "spark.graphlab.AnalyticsKryoRegistrator")

    taskType match {
      case "pagerank" => {

        var numIter = Int.MaxValue
        var isDynamic = false
        var tol:Float = 0.001F
        var outFname = ""
        var numVPart = 4
        var numEPart = 4

        options.foreach{
          case ("numIter", v) => numIter = v.toInt
          case ("dynamic", v) => isDynamic = v.toBoolean
          case ("tol", v) => tol = v.toFloat
          case ("output", v) => outFname = v
          case ("numVPart", v) => numVPart = v.toInt
          case ("numEPart", v) => numEPart = v.toInt
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        if(!isDynamic && numIter == Int.MaxValue) {
          println("Set number of iterations!")
          sys.exit(1)
        }
        println("======================================")
        println("|             PageRank               |")
        println("--------------------------------------")
        println(" Using parameters:")
        println(" \tDynamic:  " + isDynamic)
        if(isDynamic) println(" \t  |-> Tolerance: " + tol)
        println(" \tNumIter:  " + numIter)
        println("======================================")

        val sc = new SparkContext(host, "PageRank(" + fname + ")")
        val graph = Graph.textFile(sc, fname, a => 1.0F).withPartitioner(numVPart, numEPart)
        val startTime = System.currentTimeMillis

        val pr = Analytics.pagerank(graph, numIter)
        // val pr = if(isDynamic) Analytics.dynamicPagerank(graph, tol, numIter)
        //   else  Analytics.pagerank(graph, numIter)
        println("Total rank: " + pr.vertices.map{ case Vertex(id,r) => r }.reduce(_+_) )
        if(!outFname.isEmpty) {
          println("Saving pageranks of pages to " + outFname)
          pr.vertices.map{case Vertex(id, r) => id + "\t" + r}.saveAsTextFile(outFname)
        }
        println("Runtime:    " + ((System.currentTimeMillis - startTime)/1000.0) + " seconds")
        sc.stop()
      }

     case "cc" => {

        var numIter = Int.MaxValue
        var isDynamic = false

        options.foreach{
          case ("numIter", v) => numIter = v.toInt
          case ("dynamic", v) => isDynamic = v.toBoolean
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        if(!isDynamic && numIter == Int.MaxValue) {
          println("Set number of iterations!")
          sys.exit(1)
        }
        println("======================================")
        println("|      Connected Components          |")
        println("--------------------------------------")
        println(" Using parameters:")
        println(" \tDynamic:  " + isDynamic)
        println(" \tNumIter:  " + numIter)
        println("======================================")

        val sc = new SparkContext(host, "ConnectedComponents(" + fname + ")")
        val graph = Graph.textFile(sc, fname, a => 1.0F)
        val cc = Analytics.connectedComponents(graph, numIter)
        // val cc = if(isDynamic) Analytics.dynamicConnectedComponents(graph, numIter)
        //   else  Analytics.connectedComponents(graph, numIter)
        println("Components: " + cc.vertices.map(_.data).distinct())

        sc.stop()
      }

     case "shortestpath" => {

        var numIter = Int.MaxValue
        var isDynamic = true
        var sources: List[Int] = List.empty

        options.foreach{
          case ("numIter", v) => numIter = v.toInt
          case ("dynamic", v) => isDynamic = v.toBoolean
          case ("source", v) => sources ++= List(v.toInt)
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }


        if(!isDynamic && numIter == Int.MaxValue) {
          println("Set number of iterations!")
          sys.exit(1)
        }

        if(sources.isEmpty) {
          println("No sources provided!")
          sys.exit(1)
        }

        println("======================================")
        println("|          Shortest Path             |")
        println("--------------------------------------")
        println(" Using parameters:")
        println(" \tDynamic:  " + isDynamic)
        println(" \tNumIter:  " + numIter)
        println(" \tSources:  [" + sources.mkString(", ") + "]")
        println("======================================")

        val sc = new SparkContext(host, "ShortestPath(" + fname + ")")
        val graph = Graph.textFile(sc, fname, a => (if(a.isEmpty) 1.0F else a(0).toFloat ) )
        val sp = Analytics.shortestPath(graph, sources, numIter)
        // val cc = if(isDynamic) Analytics.dynamicShortestPath(graph, sources, numIter)
        //   else  Analytics.shortestPath(graph, sources, numIter)
        println("Longest Path: " + sp.vertices.map(_.data).reduce(math.max(_,_)))

        sc.stop()
      }


     //  case "als" => {

     //    var numIter = 5
     //    var lambda = 0.01
     //    var latentK = 10
     //    var usersFname = "usersFactors.tsv"
     //    var moviesFname = "moviesFname.tsv"
     //    var numVPart = 4
     //    var numEPart = 4

     //    options.foreach{
     //      case ("numIter", v) => numIter = v.toInt
     //      case ("lambda", v) => lambda = v.toDouble
     //      case ("latentK", v) => latentK = v.toInt
     //      case ("usersFname", v) => usersFname = v
     //      case ("moviesFname", v) => moviesFname = v
     //      case ("numVPart", v) => numVPart = v.toInt
     //      case ("numEPart", v) => numEPart = v.toInt
     //      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
     //    }

     //    println("======================================")
     //    println("|       Alternating Least Squares    |")
     //    println("--------------------------------------")
     //    println(" Using parameters:")
     //    println(" \tNumIter:     " + numIter)
     //    println(" \tLambda:      " + lambda)
     //    println(" \tLatentK:     " + latentK)
     //    println(" \tusersFname:  " + usersFname)
     //    println(" \tmoviesFname: " + moviesFname)
     //    println("======================================")

     //    val sc = new SparkContext(host, "ALS(" + fname + ")")
     //    val graph = Graph.textFile(sc, fname, a => a(0).toDouble )
     //    graph.numVPart = numVPart
     //    graph.numEPart = numEPart

     //    val maxUser = graph.edges.map(_._1).reduce(math.max(_,_))
     //    val minMovie = graph.edges.map(_._2).reduce(math.min(_,_))
     //    assert(maxUser < minMovie)

     //    val factors = Analytics.alternatingLeastSquares(graph, latentK, lambda, numIter).cache
     //    factors.filter(_._1 <= maxUser).map(r => r._1 + "\t" + r._2.mkString("\t"))
     //      .saveAsTextFile(usersFname)
     //    factors.filter(_._1 >= minMovie).map(r => r._1 + "\t" + r._2.mkString("\t"))
     //      .saveAsTextFile(moviesFname)

     //    sc.stop()
     //  }


      case _ => {
        println("Invalid task type.")
      }
    }

  }


}
