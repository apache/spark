package org.apache.spark.sql.proto

import java.io.ByteArrayInputStream
import com.google.protobuf.DynamicMessage
import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, SpecificInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, StructType}

import scala.util.control.NonFatal

private[proto] case class ProtoDataToCatalyst(child: Expression, simpleMessage: SimpleMessageProtos.SimpleMessage,
                                              options: Map[String, String])
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType = {
    val dt = SchemaConverters.toSqlType(expectedSchema).dataType
    parseMode match {
      // With PermissiveMode, the output Catalyst row might contain columns of null values for
      // corrupt records, even if some of the columns are not nullable in the user-provided schema.
      // Therefore we force the schema to be all nullable here.
      case PermissiveMode => dt.asNullable
      case _ => dt
    }
  }

  override def nullable: Boolean = true

  private lazy val protoOptions = ProtoOptions(options)

  @transient private lazy val descriptor = simpleMessage.getDescriptorForType

  @transient private lazy val actualSchema = descriptor

  @transient private lazy val expectedSchema = protoOptions.schema.getOrElse(descriptor)

//  @transient private lazy val reader = new GenericDatumReader[Any](actualSchema, expectedSchema)

  @transient private lazy val deserializer = new ProtoDeserializer(expectedSchema, dataType, protoOptions.datetimeRebaseModeInRead)

//  @transient private var decoder: BinaryDecoder = _

  @transient private var result: Any = _

  @transient private lazy val parseMode: ParseMode = {
    val mode = protoOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw new AnalysisException(unacceptableModeMessage(mode.name))
    }
    mode
  }

  private def unacceptableModeMessage(name: String): String = {
    s"from_proto() doesn't support the $name mode. " +
      s"Acceptable modes are ${PermissiveMode.name} and ${FailFastMode.name}."
  }

  @transient private lazy val nullResultRow: Any = dataType match {
    case st: StructType =>
      val resultRow = new SpecificInternalRow(st.map(_.dataType))
      for(i <- 0 until st.length) {
        resultRow.setNullAt(i)
      }
      resultRow

    case _ =>
      null
  }


  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      result = DynamicMessage.parseFrom(descriptor, new ByteArrayInputStream(binary))
      println(result.toString)
      println(descriptor.toProto.toString)
      val deserialized = deserializer.deserialize(result)
      assert(deserialized.isDefined,
        "Proto deserializer cannot return an empty result because filters are not pushed down")
      deserialized.get
    } catch {
      // There could be multiple possible exceptions here, e.g. java.io.IOException,
      // ProtoRuntimeException, ArrayIndexOutOfBoundsException, etc.
      // To make it simple, catch all the exceptions here.
      case NonFatal(e) => parseMode match {
        case PermissiveMode => nullResultRow
        case FailFastMode =>
          throw new SparkException("Malformed records are detected in record parsing. " +
            s"Current parse Mode: ${FailFastMode.name}. To process malformed records as null " +
            "result, try setting the option 'mode' as 'PERMISSIVE'.", e)
        case _ =>
          throw new AnalysisException(unacceptableModeMessage(parseMode.name))
      }
    }
  }

  override def prettyName: String = "from_proto"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, eval => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
        $dt $result = ($dt) $expr.nullSafeEval($eval);
        if ($result == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $result;
        }
      """
    })
  }

  override protected def withNewChildInternal(newChild: Expression): ProtoDataToCatalyst =
    copy(child = newChild)
}
