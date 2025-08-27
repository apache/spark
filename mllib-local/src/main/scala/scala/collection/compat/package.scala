// scalastyle:off license
/*
 * Scala (https://www.scala-lang.org)
 *
 * Copyright EPFL and Lightbend, Inc. dba Akka
 *
 * Licensed under Apache License 2.0
 * (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */
// scalastyle:on license

package scala.collection

package object compat {
  type Factory[-A, +C] = scala.collection.Factory[A, C]
  val Factory = scala.collection.Factory

  type BuildFrom[-From, -A, +C] = scala.collection.BuildFrom[From, A, C]
  val BuildFrom = scala.collection.BuildFrom

  type IterableOnce[+X] = scala.collection.IterableOnce[X]
  val IterableOnce = scala.collection.IterableOnce
}
