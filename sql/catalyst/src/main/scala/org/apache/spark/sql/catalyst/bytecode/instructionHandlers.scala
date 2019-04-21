/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.bytecode

import scala.util.Try

import javassist.CtField
import javassist.bytecode.ConstPool
import javassist.bytecode.Opcode._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.bytecode.BehaviorConverter.convert
import org.apache.spark.sql.catalyst.bytecode.InstructionHandler.{Action, Complete, Continue, Jump}
import org.apache.spark.sql.catalyst.expressions.{Add, CaseWhen, Cast, Divide, EqualTo, Expression, GetStructField, GreaterThan, GreaterThanOrEqual, If, IntegralDivide, IsNaN, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, Remainder, Subtract}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A bytecode instruction within a given [[Behavior]].
 *
 * @param opcode the opcode of this instruction.
 * @param index the index of this instruction in the behavior.
 * @param behavior the behavior.
 */
private[bytecode] case class Instruction(opcode: Int, index: Int, behavior: Behavior)

/**
 * The main class that handles bytecode instructions.
 */
private[bytecode] object InstructionHandler {

  sealed trait Action
  final case class Complete(returnValue: Option[Expression]) extends Action
  final case class Jump(index: Int) extends Action
  final case object Continue extends Action

  private val handler: InstructionHandler = LocalVarHandler orElse
    ConstantHandler orElse
    TypeConversionHandler orElse
    MathOperationHandler orElse
    IfStatementHandler orElse
    ComparisonHandler orElse
    InstanceMethodCallHandler orElse
    InstanceMethodCallWithDispatchHandler orElse
    StaticMethodCallHandler orElse
    StaticFieldHandler orElse
    ObjectFieldHandler orElse
    ObjectCreationHandler orElse
    MiscInstructionHandler orElse
    ReturnHandler

  def handle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Action = {

    handler.tryToHandle(instruction, operandStack, localVars) match {
      case Some(action) => action
      case None =>
        throw new RuntimeException(
          s"""The opcode at index ${instruction.index} is not supported:
             |${instruction.behavior.toDebugString}""".stripMargin)
    }
  }
}

/**
 * The base trait for all instruction handlers.
 *
 * Each handler must implement the `tryToHandle` method. If a handler can process an instruction,
 * it performs actions on the local variable array and the operand stack and returns [[Some]]
 * with a particular [[Action]], which defines next steps. If a handler cannot
 * process an instruction, `tryToHandle` should return None.
 *
 * Handlers can be chained via `orElse`.
 */
private[bytecode] trait InstructionHandler {

  def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action]

  def orElse(anotherHandler: InstructionHandler): InstructionHandler = {
    val currentHandler = this

    (i: Instruction, s: OperandStack, l: LocalVarArray) => {
      currentHandler.tryToHandle(i, s, l).orElse(anotherHandler.tryToHandle(i, s, l))
    }
  }
}

/**
 * The handler that deals with all instructions related to loading local vars onto the operand stack
 * or storing values from the operand stack into the local variable array.
 */
private[bytecode] object LocalVarHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case ALOAD_0 | ILOAD_0 | LLOAD_0 | FLOAD_0 | DLOAD_0 =>
        operandStack.push(localVars(0))
      case ALOAD_1 | ILOAD_1 | LLOAD_1 | FLOAD_1 | DLOAD_1 =>
        operandStack.push(localVars(1))
      case ALOAD_2 | ILOAD_2 | LLOAD_2 | FLOAD_2 | DLOAD_2 =>
        operandStack.push(localVars(2))
      case ALOAD_3 | ILOAD_3 | LLOAD_3 | FLOAD_3 | DLOAD_3 =>
        operandStack.push(localVars(3))
      case ALOAD | ILOAD | LLOAD | FLOAD | DLOAD =>
        val codeIter = instruction.behavior.newCodeIterator()
        val localVarIndex = codeIter.byteAt(instruction.index + 1)
        operandStack.push(localVars(localVarIndex))
      case ASTORE_0 | ISTORE_0 | LSTORE_0 | FSTORE_0 | DSTORE_0 =>
        localVars(0) = operandStack.pop()
      case ASTORE_1 | ISTORE_1 | LSTORE_1 | FSTORE_1 | DSTORE_1 =>
        localVars(1) = operandStack.pop()
      case ASTORE_2 | ISTORE_2 | LSTORE_2 | FSTORE_2 | DSTORE_2 =>
        localVars(2) = operandStack.pop()
      case ASTORE_3 | ISTORE_3 | LSTORE_3 | FSTORE_3 | DSTORE_3 =>
        localVars(3) = operandStack.pop()
      case ASTORE | ISTORE | LSTORE | FSTORE | DSTORE =>
        val codeIter = instruction.behavior.newCodeIterator()
        val localVarIndex = codeIter.byteAt(instruction.index + 1)
        localVars(localVarIndex) = operandStack.pop()
      case _ =>
        return None
    }

    Some(Continue)
  }
}

/**
 * The handler that deals with pushing constants onto the operand stack.
 */
private[bytecode] object ConstantHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case ICONST_M1 =>
        operandStack.push(Literal(-1))
      case ICONST_0 =>
        operandStack.push(Literal(0))
      case ICONST_1 =>
        operandStack.push(Literal(1))
      case ICONST_2 =>
        operandStack.push(Literal(2))
      case ICONST_3 =>
        operandStack.push(Literal(3))
      case ICONST_4 =>
        operandStack.push(Literal(4))
      case ICONST_5 =>
        operandStack.push(Literal(5))
      case LCONST_0 =>
        operandStack.push(Literal(0L))
      case LCONST_1 =>
        operandStack.push(Literal(1L))
      case FCONST_0 =>
        operandStack.push(Literal(0.0F))
      case FCONST_1 =>
        operandStack.push(Literal(1.0F))
      case FCONST_2 =>
        operandStack.push(Literal(2.0F))
      case DCONST_0 =>
        operandStack.push(Literal(0.0))
      case DCONST_1 =>
        operandStack.push(Literal(1.0))
      case LDC =>
        val codeIter = instruction.behavior.newCodeIterator()
        val constantIndex = codeIter.byteAt(instruction.index + 1)
        val constant = getConstant(constantIndex, instruction.behavior.constPool)
        operandStack.push(Literal(constant))
      case LDC_W | LDC2_W =>
        val codeIter = instruction.behavior.newCodeIterator()
        val constantIndex = codeIter.u16bitAt(instruction.index + 1)
        val constant = getConstant(constantIndex, instruction.behavior.constPool)
        val expr = constant match {
          case c if c.isInstanceOf[String] => ObjectRef(Literal(c), classOf[String])
          case c => Literal(c)
        }
        operandStack.push(expr)
      case BIPUSH =>
        val codeIter = instruction.behavior.newCodeIterator()
        val byteValue = codeIter.signedByteAt(instruction.index + 1)
        operandStack.push(Literal(byteValue, IntegerType))
      // TODO: WARNING! It is not OK to keep it NullType, we need a proper type
      // case ACONST_NULL =>
      //   operandStack.push(Literal(null, NullType))
      case _ =>
        return None
    }

    Some(Continue)
  }

  private def getConstant(index: Int, constPool: ConstPool): Any = {
    constPool.getTag(index) match {
      case ConstPool.CONST_Double => constPool.getDoubleInfo(index)
      case ConstPool.CONST_Float => constPool.getFloatInfo(index)
      case ConstPool.CONST_Integer => constPool.getIntegerInfo(index)
      case ConstPool.CONST_Long => constPool.getLongInfo(index)
      case ConstPool.CONST_String => constPool.getStringInfo(index)
    }
  }
}

/**
 * The handler that deals with type conversions.
 */
private[bytecode] object TypeConversionHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case L2I | F2I | D2I =>
        operandStack.push(Cast(operandStack.pop(), IntegerType))
      case I2L | F2L | D2L =>
        operandStack.push(Cast(operandStack.pop(), LongType))
      case I2F | L2F | D2F =>
        operandStack.push(Cast(operandStack.pop(), FloatType))
      case I2D | L2D | F2D =>
        operandStack.push(Cast(operandStack.pop(), DoubleType))
      case I2B =>
        operandStack.push(Cast(operandStack.pop(), ByteType))
      case I2S =>
        operandStack.push(Cast(operandStack.pop(), ShortType))
      case I2C =>
        // TODO: string vs char type
        operandStack.push(Cast(operandStack.pop(), StringType))
      case _ =>
        return None
    }

    Some(Continue)
  }
}

/**
 * The handler that deals with basic math operations.
 */
// TODO: nulls are handled differently in all expressions
// TODO: division by zero and other edge cases
private[bytecode] object MathOperationHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case IADD =>
        compute(Add, IntegerType, operandStack)
      case LADD =>
        compute(Add, LongType, operandStack)
      case FADD =>
        compute(Add, FloatType, operandStack)
      case DADD =>
        compute(Add, DoubleType, operandStack)
      case ISUB =>
        compute(Subtract, IntegerType, operandStack)
      case LSUB =>
        compute(Subtract, LongType, operandStack)
      case FSUB =>
        compute(Subtract, FloatType, operandStack)
      case DSUB =>
        compute(Subtract, DoubleType, operandStack)
      case IMUL =>
        compute(Multiply, IntegerType, operandStack)
      case LMUL =>
        compute(Multiply, LongType, operandStack)
      case FMUL =>
        compute(Multiply, FloatType, operandStack)
      case DMUL =>
        compute(Multiply, DoubleType, operandStack)
      case IDIV =>
        compute((left, right) => IntegralDivide(left, right), IntegerType, operandStack)
      case LDIV =>
        compute((left, right) => IntegralDivide(left, right), LongType, operandStack)
      case FDIV =>
        // TODO
        // for now, fetch as doubles because Divide can process only doubles and decimals
        compute((left, right) => Cast(Divide(left, right), FloatType), DoubleType, operandStack)
      case DDIV =>
        compute(Divide, DoubleType, operandStack)
      case IREM =>
        compute(Remainder, IntegerType, operandStack)
      case LREM =>
        compute(Remainder, LongType, operandStack)
      case FREM =>
        compute(Remainder, FloatType, operandStack)
      case DREM =>
        compute(Remainder, DoubleType, operandStack)
      case _ =>
        return None
    }

    Some(Continue)
  }

  private def compute(
      func: (Expression, Expression) => Expression,
      dataType: AtomicType,
      operandStack: OperandStack): Unit = {

    val rightOperand = getOperand(dataType, operandStack)
    val leftOperand = getOperand(dataType, operandStack)
    operandStack.push(func(leftOperand, rightOperand))
  }

  // when we have "1.toByte + 1", we need to promote the byte value to Int (just as Java/Scala do)
  // otherwise, the result expression will be unresolved
  private def getOperand(targetType: AtomicType, operandStack: OperandStack): Expression = {
    val operand = operandStack.pop()
    operand.dataType match {
      case operandType: AtomicType if operandType != targetType =>
        require(Cast.canSafeCast(operandType, targetType))
        Cast(operand, targetType)
      case operandType: AtomicType if operandType == targetType =>
        operand
      case _ =>
        throw new RuntimeException(s"$operand cannot be safely cast to $targetType")
    }
  }
}

/**
 * The handler that deals with if statements. As some if conditions cannot be evaluated right away,
 * we need to recursively transform both true and false branches and then construct an if statement.
 */
// TODO: this logic must be absolutely revisited!
// TODO: avoid recursion if the condition can be evaluated immediately
private[bytecode] object IfStatementHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    def computeIfExpression(cond: Expression): Option[Action] = {
      val behavior = instruction.behavior
      val codeIter = behavior.newCodeIterator()

      val trueExprIndex = codeIter.s16bitAt(instruction.index + 1) + instruction.index
      val trueExpr = convert(trueExprIndex, behavior, operandStack.clone(), localVars)

      val falseExprIndex = instruction.index + 3
      val falseExpr = convert(falseExprIndex, behavior, operandStack.clone(), localVars)

      if (trueExpr.isEmpty || falseExpr.isEmpty) {
        throw new RuntimeException("If statements with void branches are not supported")
      }

      val returnValue = If(cond, trueExpr.get, falseExpr.get) match {
        case e if !e.resolved =>
          throw new RuntimeException(s"$e is not supported")
        case e @ If(_, t: Ref, f: Ref) if t.clazz != f.clazz =>
          throw new RuntimeException(s"$e is not supported")
        case e @ If(_, t: Ref, _: Ref) =>
          ObjectRef(e, t.clazz)
        case e @ If(_, _: Ref, _) =>
          throw new RuntimeException(s"$e is not supported")
        case e @ If(_, _, _: Ref) =>
          throw new RuntimeException(s"$e is not supported")
        case e =>
          e
      }

      Some(Complete(Some(returnValue)))
    }

    instruction.opcode match {
      case IFEQ =>
        val top = operandStack.pop()
        computeIfExpression(EqualTo(top, Cast(Literal(0), top.dataType)))
      case IFNE =>
        val top = operandStack.pop()
        computeIfExpression(Not(EqualTo(top, Cast(Literal(0), top.dataType))))
      case IFGT =>
        val top = operandStack.pop()
        computeIfExpression(GreaterThan(top, Cast(Literal(0), top.dataType)))
      case IFGE =>
        val top = operandStack.pop()
        computeIfExpression(GreaterThanOrEqual(top, Cast(Literal(0), top.dataType)))
      case IFLT =>
        val top = operandStack.pop()
        computeIfExpression(LessThan(top, Cast(Literal(0), top.dataType)))
      case IFLE =>
        val top = operandStack.pop()
        computeIfExpression(LessThanOrEqual(top, Cast(Literal(0), top.dataType)))
      case IFNONNULL =>
        val top = operandStack.pop()
        computeIfExpression(IsNotNull(top))
      case IFNULL =>
        val top = operandStack.pop()
        computeIfExpression(IsNull(top))
      case IF_ACMPEQ | IF_ICMPEQ =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(EqualTo(leftOperand, rightOperand))
      case IF_ACMPNE | IF_ICMPNE =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(Not(EqualTo(leftOperand, rightOperand)))
      case IF_ICMPGE =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(GreaterThanOrEqual(leftOperand, rightOperand))
      case IF_ICMPGT =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(GreaterThan(leftOperand, rightOperand))
      case IF_ICMPLE =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(LessThanOrEqual(leftOperand, rightOperand))
      case IF_ICMPLT =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(LessThan(leftOperand, rightOperand))
      case _ =>
        None
    }
  }
}

/**
 * The handler that is responsible for comparing items on the operand stack.
 *
 * FCMPG vs FCMPL and DCMPG vs DCMPL differ only in handling NaN values.
 */
private[bytecode] object ComparisonHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case LCMP =>
        // TODO is it OK to have positive/negative or we need to have -1,0,1 only?
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        operandStack.push(Subtract(leftOperand, rightOperand))
      case FCMPG | DCMPG =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        val branches = Seq(
          Or(IsNaN(leftOperand), IsNaN(rightOperand)) -> Literal(1),
          GreaterThan(leftOperand, rightOperand) -> Literal(1),
          LessThan(leftOperand, rightOperand) -> Literal(-1))
        operandStack.push(CaseWhen(branches, Literal(0)))
      case FCMPL | DCMPL =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        val branches = Seq(
          Or(IsNaN(leftOperand), IsNaN(rightOperand)) -> Literal(-1),
          GreaterThan(leftOperand, rightOperand) -> Literal(1),
          LessThan(leftOperand, rightOperand) -> Literal(-1))
        operandStack.push(CaseWhen(branches, Literal(0)))
      case _ =>
        return None
    }

    Some(Continue)
  }
}

/**
 * The base trait for handlers that deal with fields.
 */
private[bytecode] trait FieldHandler extends InstructionHandler {

  protected def getField(fieldIndex: Int, constPool: ConstPool): CtField = {
    val className = constPool.getFieldrefClassName(fieldIndex)
    val fieldName = constPool.getFieldrefName(fieldIndex)
    val ctClass = CtClassPool.getCtClass(className)
    ctClass.getField(fieldName)
  }
}

/**
 * The handler that deals with static variables.
 */
private[bytecode] object StaticFieldHandler extends FieldHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    // TODO: Java statics
    instruction.opcode match {
      case GETSTATIC =>
        val codeIter = instruction.behavior.newCodeIterator()
        val constPool = instruction.behavior.constPool
        val fieldIndex = codeIter.u16bitAt(instruction.index + 1)
        val field = getField(fieldIndex, constPool)
        // right now, only Scala objects are supported
        if (field.getName == "MODULE$") {
          // TODO: handle stateful objects
          val className = field.getDeclaringClass.getName
          val scalaObjectRef = ScalaObjectRef(Utils.classForName(className))
          operandStack.push(scalaObjectRef)
          Some(Continue)
        } else {
          throw new RuntimeException(s"Getting arbitrary static fields is not supported: $field")
        }
      case PUTSTATIC =>
        throw new RuntimeException(s"Setting static fields is not supported")
      case _ =>
        None
    }
  }
}

/**
 * The handler that deals with object fields.
 */
private[bytecode] object ObjectFieldHandler extends FieldHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case GETFIELD =>
        val codeIter = instruction.behavior.newCodeIterator()
        val constPool = instruction.behavior.constPool
        val fieldIndex = codeIter.u16bitAt(instruction.index + 1)
        val field = getField(fieldIndex, constPool)
        val fieldName = field.getName
        val obj = operandStack.pop()
        val fieldValue = getFieldValue(obj, fieldName)
        operandStack.push(fieldValue)
        Some(Continue)
      case PUTFIELD =>
        val codeIter = instruction.behavior.newCodeIterator()
        val constPool = instruction.behavior.constPool
        val fieldIndex = codeIter.u16bitAt(instruction.index + 1)
        val field = getField(fieldIndex, constPool)
        val fieldName = field.getName
        val (fieldValue, obj) = (operandStack.pop(), operandStack.pop())
        setFieldValue(obj, fieldName, fieldValue)
        Some(Continue)
      case _ =>
        None
    }
  }

  private def getFieldValue(operand: Expression, fieldName: String): Expression = operand match {
    case ObjectRef(Literal(null, structType: StructType), _) =>
      val field = structType(fieldName)
      AssertNotNull(Literal(null, field.dataType))
    case e: StructRef =>
      e.getFieldValue(fieldName)
    case e: ObjectRef if e.isBoxedPrimitive && fieldName == "value" =>
      e.value
    case e: ObjectRef if e.dataType.isInstanceOf[StructType] =>
      val ctClass = CtClassPool.getCtClass(e.clazz)
      val structType = e.dataType.asInstanceOf[StructType]
      val fieldIndex = structType.fieldIndex(fieldName)
      // TODO: field desc?
      val ctField = ctClass.getField(fieldName)
      val fieldValue = GetStructField(e.value, fieldIndex)
      if (ctField.getType.isPrimitive) {
        fieldValue
      } else {
        val fieldClazz = Utils.classForName(ctField.getType.getName)
        ObjectRef(fieldValue, fieldClazz)
      }
    case _ =>
      throw new RuntimeException(s"Cannot get field '$fieldName' from '$operand'")
  }

  private def setFieldValue(obj: Expression, fieldName: String, value: Expression): Unit = {
    obj match {
      case e: StructRef =>
        e.setFieldValue(fieldName, value)
      // TODO: proper validation
      case e: ObjectRef if e.isBoxedPrimitive && fieldName == "value" =>
        e.value = value
      case _ =>
        throw new RuntimeException(s"Cannot set '$fieldName' in '$obj'")
    }
  }
}

/**
 * The handler that deals with creating new objects.
 */
private[bytecode] object ObjectCreationHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case NEW =>
        val codeIter = instruction.behavior.newCodeIterator()
        val constPool = instruction.behavior.constPool
        val classIndex = codeIter.u16bitAt(instruction.index + 1)
        val className = constPool.getClassInfo(classIndex)
        val clazz = Utils.classForName(className)
        if (boxedPrimitiveClasses.contains(className)) {
          operandStack.push(newNullObjectRef(clazz))
        } else {
          operandStack.push(StructRef(clazz))
        }
        Some(Continue)
      case _ =>
        None
    }
  }

  private def newNullObjectRef(clazz: Class[_]): ObjectRef = {
    val dataType = ScalaReflection.schemaFor(clazz).dataType
    val value = Literal(null, dataType)
    ObjectRef(value, clazz)
  }
}

/**
 * The base trait for handlers that deal with method/constructor invocations.
 */
private[bytecode] trait InvokeHandler extends InstructionHandler {

  protected def getInvokeTarget(instruction: Instruction): Behavior = {
    val codeIter = instruction.behavior.newCodeIterator()
    val constPool = instruction.behavior.constPool
    val targetIndex = codeIter.u16bitAt(instruction.index + 1)
    val targetName = constPool.getMethodrefName(targetIndex)
    val targetClassName = constPool.getMethodrefClassName(targetIndex)
    val descriptor = constPool.getMethodrefType(targetIndex)
    val ctClass = CtClassPool.getCtClass(targetClassName)
    val method = Try(ctClass.getMethod(targetName, descriptor))
    val constructor = Try(ctClass.getConstructor(descriptor))
    Behavior(method.orElse(constructor).get)
  }

  protected def popArguments(number: Int, operandStack: OperandStack): Array[Expression] = {
    val args = new Array[Expression](number)
    for (index <- number - 1 to 0 by -1) {
      args(index) = operandStack.pop()
    }
    args
  }
}

/**
 * The handler that deals with instance method calls without method dispatch.
 */
private[bytecode] object InstanceMethodCallHandler extends InvokeHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    // TODO: proper null handling
    instruction.opcode match {
      case INVOKESPECIAL =>
        val target = getInvokeTarget(instruction)
        val argsNum = target.numParameters
        val args = popArguments(argsNum, operandStack)
        val thisRef = operandStack.pop()
        val targetLocalVars = newLocalVarArray(target, Some(thisRef), args)
        val targetResult = convert(target, targetLocalVars)
        targetResult.foreach(operandStack.push)
        Some(Continue)
      case _ =>
        None
    }
  }
}

/**
 * The handler that deals with instance method calls with method dispatch.
 */
private[bytecode] object InstanceMethodCallWithDispatchHandler extends InvokeHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    // TODO: proper null handling
    instruction.opcode match {
      case INVOKEVIRTUAL | INVOKEINTERFACE =>
        val compileTarget = getInvokeTarget(instruction)
        val argsNum = compileTarget.numParameters
        val args = popArguments(argsNum, operandStack)
        val thisRef = operandStack.pop()
        // we simulate method dispatch
        val runtimeTarget = thisRef match {
          case ObjectRef(Literal(null, _), _) =>
            throw new AnalysisException(s"Calling '${compileTarget.name}' on a null reference")
          case e: Ref =>
            val runtimeClass = CtClassPool.getCtClass(e.clazz)
            runtimeClass.getMethod(compileTarget.name, compileTarget.signature)
          case _ =>
            throw new RuntimeException(s"Unsupported `thisRef` for invokevirtual: $thisRef")
        }
        val targetLocalVars = newLocalVarArray(runtimeTarget, Some(thisRef), args)
        val targetResult = convert(runtimeTarget, targetLocalVars)
        targetResult.foreach(operandStack.push)
        Some(Continue)
      case _ =>
        None
    }
  }
}

/**
 * The handler that deals with static method calls.
 */
private[bytecode] object StaticMethodCallHandler extends InvokeHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case INVOKESTATIC =>
        val target = getInvokeTarget(instruction)
        val argsNum = target.numParameters
        val args = popArguments(argsNum, operandStack)
        val targetLocalVars = newLocalVarArray(target, None, args)
        val targetResult = convert(target, targetLocalVars)
        targetResult.foreach(operandStack.push)
        Some(Continue)
      case _ =>
        None
    }

  }
}

/**
 * The handler that deals with miscellaneous instructions.
 */
private[bytecode] object MiscInstructionHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    instruction.opcode match {
      case DUP =>
        operandStack.push(operandStack.top)
        Some(Continue)
      case DUP2 =>
        operandStack.top match {
          case _: Ref =>
            val (topOperand, nextOperand) = (operandStack.pop(), operandStack.pop())
            operandStack.push(nextOperand)
            operandStack.push(topOperand)
            operandStack.push(nextOperand)
            operandStack.push(topOperand)
          case e if e.dataType == LongType || e.dataType == DoubleType =>
            operandStack.push(operandStack.top)
        }
        Some(Continue)
      case POP =>
        operandStack.pop()
        Some(Continue)
      case POP2 =>
        operandStack.top match {
          case _: Ref =>
            operandStack.pop()
            operandStack.pop()
          case e if e.dataType == LongType || e.dataType == DoubleType =>
            operandStack.pop()
        }
        Some(Continue)
      case GOTO =>
        val codeIter = instruction.behavior.newCodeIterator()
        val targetIndex = codeIter.s16bitAt(instruction.index + 1) + instruction.index
        Some(Jump(targetIndex))
      case CHECKCAST =>
        val codeIter = instruction.behavior.newCodeIterator()
        val classIndex = codeIter.u16bitAt(instruction.index + 1)
        val constPool = instruction.behavior.constPool
        val className = constPool.getClassInfo(classIndex)
        val clazz = Utils.classForName(className)
        val dataType = ScalaReflection.schemaFor(clazz).dataType
        // TODO: this should throw ClassCastException in JVM
        require(operandStack.top.dataType == dataType)
        Some(Continue)
      case _ =>
        None
    }
  }
}

/**
 * The handler that deals with return statements.
 */
private[bytecode] object ReturnHandler extends InstructionHandler {

  override def tryToHandle(
      instruction: Instruction,
      operandStack: OperandStack,
      localVars: LocalVarArray): Option[Action] = {

    val behavior = instruction.behavior
    val returnType = behavior.returnType

    instruction.opcode match {
      case IRETURN if returnType.exists(_.getName == "boolean") =>
        // TODO: will it be always 1 or 0? Can we simply replace it with true/false?
        Some(Complete(returnValue = Some(Cast(operandStack.pop, BooleanType))))
      case IRETURN if returnType.exists(_.getName == "byte") =>
        Some(Complete(returnValue = Some(Cast(operandStack.pop, ByteType))))
      case IRETURN if returnType.exists(_.getName == "short") =>
        Some(Complete(returnValue = Some(Cast(operandStack.pop, ShortType))))
      case ARETURN | IRETURN | LRETURN | FRETURN | DRETURN =>
        Some(Complete(returnValue = Some(operandStack.pop)))
      case RETURN =>
        Some(Complete(returnValue = None))
      case _ =>
        None
    }
  }
}
