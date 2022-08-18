package org.apache.spark.sql.proto

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column

object functions {
  @Experimental
  def from_proto(data: Column, descriptorBytes: SimpleMessageProtos.SimpleMessage): Column = {
    new Column(ProtoDataToCatalyst(data.expr, descriptorBytes, Map.empty))
  }
}
