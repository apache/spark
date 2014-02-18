package org.apache.spark.sql

/**
 * An execution engine for relational query plans that runs on top Spark and returns RDDs.
 */
package object execution {
  type Row = catalyst.expressions.Row
}
