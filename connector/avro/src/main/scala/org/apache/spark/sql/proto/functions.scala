package org.apache.spark.sql.proto

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column

object functions {

  /**
   * Converts a binary column of Proto format into its corresponding catalyst value.
   * The specified schema must match actual schema of the read data, otherwise the behavior
   * is undefined: it may fail or return arbitrary result.
   * To deserialize the data with a compatible and evolved schema, the expected Avro schema can be
   * set via the option avroSchema.
   *
   * @param data             the binary column.
   * @param descriptorBytes the proto schema in Message GeneratedMessageV3 format.
   * @param options          options to control how the Avro record is parsed.
   * @since 3.0.0
   */
  @Experimental
  def from_proto(data: Column, descriptor: SimpleMessageProtos.SimpleMessage): Column = {
    new Column(ProtoDataToCatalyst(data.expr, descriptor, Map.empty))
  }

  /**
   * Converts a column into binary of proto format.
   *
   * @param data the data column.
   * @since 3.0.0
   */
  @Experimental
  def to_proto(data: Column, descriptor: SimpleMessageProtos.SimpleMessage): Column = {
    new Column(CatalystDataToProto(data.expr, descriptor))
  }
}
