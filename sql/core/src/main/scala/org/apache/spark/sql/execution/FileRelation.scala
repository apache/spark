package org.apache.spark.sql.execution

import org.apache.spark.annotation.Experimental

/**
 * An interface for relations that are backed by files.  When a class implements this interface,
 * the list of paths that it returns will be returned to a user who calls `inputPaths` on any
 * DataFrame that queries this relation.
 */
@Experimental
private[sql] trait FileRelation {
  /** Returns the list of files that will be read when scanning this relation. */
  def inputFiles: Array[String]
}
