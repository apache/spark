
/**
 * Catalyst is a framework for performing optimization on trees of dataflow operators.
 */
package object catalyst {
  protected[catalyst] def Logger(name: String) =
    com.typesafe.scalalogging.slf4j.Logger(org.slf4j.LoggerFactory.getLogger(name))
  protected[catalyst] type Logging = com.typesafe.scalalogging.slf4j.Logging
}