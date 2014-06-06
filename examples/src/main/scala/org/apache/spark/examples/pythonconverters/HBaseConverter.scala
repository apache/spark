package org.apache.spark.examples.pythonconverters

import org.apache.spark.api.python.Converter
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts a HBase Result
 * to a String
 */
class HBaseConverter extends Converter[Any, String] {
  override def convert(obj: Any): String = {
    val result = obj.asInstanceOf[Result]
    Bytes.toStringBinary(result.value())
  }
}
