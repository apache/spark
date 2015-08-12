package org.apache.spark.sql.execution

/**
 * An interface for relations that are backed by files.  When a class implements this interface,
 * the list of paths that it returns will be returned to a user who calls `inputPaths` on any
 * DataFrame that queries this relation.
 */
private[sql] trait FileRelation {
  /** Returns the list of files that will be read when scanning this relation. */
  def inputFiles: Array[String]
}
