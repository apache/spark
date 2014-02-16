package org.apache.spark

/**
 * Allows the execution of relational queries, including those expressed in SQL using Spark.
 *
 * Note that this package is located in catalyst instead of in core so that all subprojects can
 * inherit the settings from this package object.
 */
package object sql {

  protected[sql] def Logger(name: String) =
    com.typesafe.scalalogging.slf4j.Logger(org.slf4j.LoggerFactory.getLogger(name))

  protected[sql] type Logging = com.typesafe.scalalogging.slf4j.Logging
}
