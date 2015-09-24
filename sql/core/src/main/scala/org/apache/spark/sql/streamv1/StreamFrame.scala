package org.apache.spark.sql.streamv1

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}


/**
 * Version A: A single StreamFrame abstraction to represent unwindowed stream and windowed stream.
 *
 * There are two alternatives for the blocking operations in StreamFrame when it is not windowed:
 *
 * A1. Blocking operations return new StreamFrames, and emits a new tuple for every update.
 *     As an example, sf.groupby("id").count() will emit a tuple every time we see a new record
 *     for "id", i.e. a running count. Note that these operations will be expensive, because
 *     they require ordering all the inputs by time.
 *
 * A2. Blocking operation throw runtime exceptions saying they are not supported.
 */
class StreamFrame {

  /////////////////////////////////////////////////////////////////////
  // Meta operations
  /////////////////////////////////////////////////////////////////////

  def schema: StructType = ???

  def dtypes: Array[(String, String)] = ???

  def columns: Array[String] = ???

  def printSchema(): Unit = ???

  def explain(extended: Boolean): Unit = ???

  /////////////////////////////////////////////////////////////////////
  // Window specification
  /////////////////////////////////////////////////////////////////////

  def window(window: WindowSpec): StreamFrame = ???

  /////////////////////////////////////////////////////////////////////
  // Pipelined operations:
  // - works only within a bounded dataset.
  // - throws runtime exception if called on an unbounded dataset.
  /////////////////////////////////////////////////////////////////////

  def select(cols: Column*): StreamFrame = ???

  def filter(condition: Column): StreamFrame = ???

  def drop(col: Column): StreamFrame = ???

  def withColumn(colName: String, col: Column): StreamFrame = ???

  def withColumnRenamed(existingName: String, newName: String): StreamFrame = ???

  def join(right: DataFrame): StreamFrame = ???

  def write: StreamFrameWriter = ???

  /////////////////////////////////////////////////////////////////////
  // Blocking operations: works only within a window
  /////////////////////////////////////////////////////////////////////

  def agg(exprs: Column*): StreamFrame = ???

  def groupby(cols: Column*): GroupedStreamFrame = ???

  def cube(cols: Column*): GroupedStreamFrame = ???

  def rollup(cols: Column*): GroupedStreamFrame = ???

  def sort(sortExprs: Column*): StreamFrame = ???

  def dropDuplicates(colNames: Seq[String]): StreamFrame = ???

  def distinct(): StreamFrame = ???
}


class GroupedStreamFrame {

  def agg(exprs: Column*): StreamFrame = ???

  def avg(colNames: String*): StreamFrame = ???

  // ...
}


class StreamFrameWriter {

}
