package org.apache.spark.graph.cypher.conversions

import org.apache.spark.graph.cypher.{SparkCypherNode, SparkCypherRelationship}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders.kryo
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}

import scala.language.implicitConversions

object CypherValueEncoders {

  private implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = {
    v.asInstanceOf[ExpressionEncoder[T]]
  }

  implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = {
    kryo[CypherValue]
  }

  implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = {
    kryo[Map[String, CypherValue]]
  }

  implicit def cypherNodeEncoder: ExpressionEncoder[SparkCypherNode] = {
    kryo[SparkCypherNode].asInstanceOf[ExpressionEncoder[SparkCypherNode]]
  }

  implicit def cypherRelationshipEncoder: ExpressionEncoder[SparkCypherRelationship] = {
    kryo[SparkCypherRelationship]
  }

  implicit def cypherMapEncoder: ExpressionEncoder[CypherMap] = {
    kryo[CypherMap]
  }
}
