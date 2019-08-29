package org.apache.spark.sql.hive.thriftserver.cli

import org.apache.hive.service.cli.thrift.TRowSet

import org.apache.spark.sql.Row

trait RowSet {
  def addRow(row: Row): RowSet

  def extractSubset(maxRows: Int): RowSet

  def numColumns: Int

  def numRows: Int

  def getStartOffset: Long

  def setStartOffset(startOffset: Long): Unit

  def toTRowSet: TRowSet
}
