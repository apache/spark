package org.apache.spark.sql.proto

import com.google.protobuf.DynamicMessage

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType}

private[proto] case class CatalystDataToProto(
                                             child: Expression,
                                             simpleMessage: SimpleMessageProtos.SimpleMessage) extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val protoType = simpleMessage.getDescriptorForType

  @transient private lazy val serializer = new ProtoSerializer(child.dataType, protoType, child.nullable)

  override def nullSafeEval(input: Any): Any = {
    val dynamicMessage = serializer.serialize(input).asInstanceOf[DynamicMessage]
    dynamicMessage.toByteArray
  }

  override def prettyName: String = "to_proto"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }

  override protected def withNewChildInternal(newChild: Expression): CatalystDataToProto =
    copy(child = newChild)
}

