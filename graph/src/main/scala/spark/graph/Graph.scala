package spark.graph

import spark.RDD


abstract class Graph[VD: ClassManifest, ED: ClassManifest] {

  def vertices(): RDD[Vertex[VD]]

  def edges(): RDD[Edge[ED]]

  def edgesWithVertices(): RDD[EdgeWithVertices[VD, ED]]

  def cache(): Graph[VD, ED]

  def mapVertices[VD2: ClassManifest](f: Vertex[VD] => VD2): Graph[VD2, ED]

  def mapEdges[ED2: ClassManifest](f: Edge[ED] => ED2): Graph[VD, ED2]

  /** Return a new graph with its edge directions reversed. */
  def reverse: Graph[VD, ED]

  def aggregateNeighbors[VD2: ClassManifest](
      mapFunc: (Vid, EdgeWithVertices[VD, ED]) => Option[VD2],
      reduceFunc: (VD2, VD2) => VD2,
      gatherDirection: EdgeDirection)
    : RDD[(Vid, VD2)]

  def aggregateNeighbors[VD2: ClassManifest](
      mapFunc: (Vid, EdgeWithVertices[VD, ED]) => Option[VD2],
      reduceFunc: (VD2, VD2) => VD2,
      default: VD2, // Should this be a function or a value?
      gatherDirection: EdgeDirection)
    : RDD[(Vid, VD2)]

  def updateVertices[U: ClassManifest, VD2: ClassManifest](
      updates: RDD[(Vid, U)],
      updateFunc: (Vertex[VD], Option[U]) => VD2)
    : Graph[VD2, ED]

  // This one can be used to skip records when we can do in-place update.
  // Annoying that we can't rename it ...
  def updateVertices2[U: ClassManifest](
      updates: RDD[(Vid, U)],
      updateFunc: (Vertex[VD], U) => VD)
    : Graph[VD, ED]

  // Save a copy of the GraphOps object so there is always one unique GraphOps object
  // for a given Graph object, and thus the lazy vals in GraphOps would work as intended.
  val ops = new GraphOps(this)
}


object Graph {

  implicit def graphToGraphOps[VD: ClassManifest, ED: ClassManifest](g: Graph[VD, ED]) = g.ops
}
