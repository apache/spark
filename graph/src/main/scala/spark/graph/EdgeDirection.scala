package spark.graph


object EdgeDirection extends Enumeration {

  type EdgeDirection = Value

  val None, In, Out, Both = Value
}
