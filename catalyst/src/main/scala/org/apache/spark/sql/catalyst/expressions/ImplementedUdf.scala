package org.apache.spark.sql
package catalyst
package expressions

/**
 * A UDF that has a native JVM implementation.
 */
trait ImplementedUdf {
  def evaluate(evaluatedChildren: Seq[Any]): Any
}
