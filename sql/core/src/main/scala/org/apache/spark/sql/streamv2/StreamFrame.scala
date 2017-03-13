package org.apache.spark.sql.streamv2

import org.apache.spark.sql.streamv1.WindowSpec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Column}


/**
 * Version B: A StreamFrame, and a WindowedStreamFrame, which can be created by StreamFrame.window.
 *
 * Blocking operations are only available on WindowedStreamFrame.
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

  def window(window: WindowSpec): WindowedStreamFrame = ???

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

}


/**
 * A WindowedStreamFrame can run all the operations available on StreamFrame, and also blocking
 * operations.
 */
class WindowedStreamFrame extends StreamFrame {

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
