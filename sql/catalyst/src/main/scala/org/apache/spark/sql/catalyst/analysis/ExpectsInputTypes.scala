package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.types.DataType

/**
 * For expressions that require a specific `DataType` as input should implement this trait
 * so that the proper type conversions can be performed in the analyzer.
 */
trait ExpectsInputTypes {
  
  def expectedChildTypes: Seq[DataType]
  
}
