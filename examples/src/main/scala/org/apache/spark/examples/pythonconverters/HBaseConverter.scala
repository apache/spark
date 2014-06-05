package org.apache.spark.examples.pythonconverters

import org.apache.spark.api.python.Converter
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

class HBaseConverter extends Converter {
  override def convert(obj: Any) = {
    val result = obj.asInstanceOf[Result]
    Bytes.toStringBinary(result.value())
  }
}
