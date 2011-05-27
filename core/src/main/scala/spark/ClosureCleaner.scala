package spark

import scala.collection.mutable.Map
import scala.collection.mutable.Set

import org.objectweb.asm.{ClassReader, MethodVisitor, Type}
import org.objectweb.asm.commons.EmptyVisitor
import org.objectweb.asm.Opcodes._


object ClosureCleaner extends Logging {
  private def getClassReader(cls: Class[_]): ClassReader = {
    new ClassReader(cls.getResourceAsStream(
      cls.getName.replaceFirst("^.*\\.", "") + ".class"))
  }
  
  private def getOuterClasses(obj: AnyRef): List[Class[_]] = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      return f.getType :: getOuterClasses(f.get(obj))
    }
    return Nil
  }
  
  private def getOuterObjects(obj: AnyRef): List[AnyRef] = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      return f.get(obj) :: getOuterObjects(f.get(obj))
    }
    return Nil
  }
  
  private def getInnerClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    var stack = List[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.head)
      stack = stack.tail
      val set = Set[Class[_]]()
      cr.accept(new InnerClosureFinder(set), 0)
      for (cls <- set -- seen) {
        seen += cls
        stack = cls :: stack
      }
    }
    return (seen - obj.getClass).toList
  }
  
  private def createNullValue(cls: Class[_]): AnyRef = {
    if (cls.isPrimitive)
      new java.lang.Byte(0: Byte) // Should be convertible to any primitive type
    else
      null
  }
  
  def clean(func: AnyRef): Unit = {
    // TODO: cache outerClasses / innerClasses / accessedFields
    val outerClasses = getOuterClasses(func)
    val innerClasses = getInnerClasses(func)
    val outerObjects = getOuterObjects(func)
    
    val accessedFields = Map[Class[_], Set[String]]()
    for (cls <- outerClasses)
      accessedFields(cls) = Set[String]()
    for (cls <- func.getClass :: innerClasses)
      getClassReader(cls).accept(new FieldAccessFinder(accessedFields), 0)

    val isInterpNull = {
      try {
        val klass = Class.forName("spark.repl.Main")
        klass.getMethod("interp").invoke(null) == null
      } catch {
        case _: ClassNotFoundException => true
      }
    }

    var outer: AnyRef = null
    for ((cls, obj) <- (outerClasses zip outerObjects).reverse) {
      outer = instantiateClass(cls, outer, isInterpNull);
      for (fieldName <- accessedFields(cls)) {
        val field = cls.getDeclaredField(fieldName)
        field.setAccessible(true)
        val value = field.get(obj)
        //logInfo("1: Setting " + fieldName + " on " + cls + " to " + value);
        field.set(outer, value)
      }
    }
    
    if (outer != null) {
      //logInfo("2: Setting $outer on " + func.getClass + " to " + outer);
      val field = func.getClass.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(func, outer)
    }
  }
  
  private def instantiateClass(cls: Class[_], outer: AnyRef, isInterpNull: Boolean): AnyRef = {
    if (isInterpNull) {
      // This is a bona fide closure class, whose constructor has no effects
      // other than to set its fields, so use its constructor
      val cons = cls.getConstructors()(0)
      val params = cons.getParameterTypes.map(createNullValue).toArray
      if (outer != null)
        params(0) = outer // First param is always outer object
      return cons.newInstance(params: _*).asInstanceOf[AnyRef]
    } else {
      // Use reflection to instantiate object without calling constructor
      val rf = sun.reflect.ReflectionFactory.getReflectionFactory();
      val parentCtor = classOf[java.lang.Object].getDeclaredConstructor();
      val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
      val obj = newCtor.newInstance().asInstanceOf[AnyRef];
      if (outer != null) {
        //logInfo("3: Setting $outer on " + cls + " to " + outer);
        val field = cls.getDeclaredField("$outer")
        field.setAccessible(true)
        field.set(obj, outer)
      }
      return obj
    }
  }
}


class FieldAccessFinder(output: Map[Class[_], Set[String]]) extends EmptyVisitor {
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    return new EmptyVisitor {
      override def visitFieldInsn(op: Int, owner: String, name: String,
          desc: String) {
        if (op == GETFIELD)
          for (cl <- output.keys if cl.getName == owner.replace('/', '.'))
            output(cl) += name
      }
      
      override def visitMethodInsn(op: Int, owner: String, name: String,
          desc: String) {
        if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer"))
          for (cl <- output.keys if cl.getName == owner.replace('/', '.'))
            output(cl) += name
      }
    }
  }
}


class InnerClosureFinder(output: Set[Class[_]]) extends EmptyVisitor {
  var myName: String = null
  
  override def visit(version: Int, access: Int, name: String, sig: String,
      superName: String, interfaces: Array[String]) {
    myName = name
  }
  
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    return new EmptyVisitor {
      override def visitMethodInsn(op: Int, owner: String, name: String,
          desc: String) {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.length > 0
            && argTypes(0).toString.startsWith("L") // is it an object?
            && argTypes(0).getInternalName == myName)
          output += Class.forName(owner.replace('/', '.'), false,
                                  Thread.currentThread.getContextClassLoader)
      }
    }
  }
}
