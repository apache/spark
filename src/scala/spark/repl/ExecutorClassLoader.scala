package spark.repl

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{URI, URL, URLClassLoader}
import java.util.concurrent.{Executors, ExecutorService}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.objectweb.asm._
import org.objectweb.asm.commons.EmptyVisitor
import org.objectweb.asm.Opcodes._


// A ClassLoader that reads classes from a Hadoop FileSystem URL, used to load
// classes defined by the interpreter when the REPL is in use
class ExecutorClassLoader(classDir: String, parent: ClassLoader)
extends ClassLoader(parent) {
  val fileSystem = FileSystem.get(new URI(classDir), new Configuration())
  val directory = new URI(classDir).getPath
  
  override def findClass(name: String): Class[_] = {
    try {
      //println("repl.ExecutorClassLoader resolving " + name)
      val path = new Path(directory, name.replace('.', '/') + ".class")
      val bytes = readAndTransformClass(name, fileSystem.open(path))
      return defineClass(name, bytes, 0, bytes.length)
    } catch {
      case e: Exception => throw new ClassNotFoundException(name, e)
    }
  }
  
  def readAndTransformClass(name: String, in: InputStream): Array[Byte] = {
    if (name.startsWith("line") && name.endsWith("$iw$")) {
      // Class seems to be an interpreter "wrapper" object storing a val or var.
      // Replace its constructor with a dummy one that does not run the
      // initialization code placed there by the REPL. The val or var will
      // be initialized later through reflection when it is used in a task.
      val cr = new ClassReader(in)
      val cw = new ClassWriter(
        ClassWriter.COMPUTE_FRAMES + ClassWriter.COMPUTE_MAXS)
      val cleaner = new ConstructorCleaner(name, cw)
      cr.accept(cleaner, 0)
      return cw.toByteArray
    } else {
      // Pass the class through unmodified
      val bos = new ByteArrayOutputStream
      val bytes = new Array[Byte](4096)
      var done = false
      while (!done) {
        val num = in.read(bytes)
        if (num >= 0)
          bos.write(bytes, 0, num)
        else
          done = true
      }
      return bos.toByteArray
    }
  }
}

class ConstructorCleaner(className: String, cv: ClassVisitor)
extends ClassAdapter(cv) {
  override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
    val mv = cv.visitMethod(access, name, desc, sig, exceptions)
    if (name == "<init>" && (access & ACC_STATIC) == 0) {
      // This is the constructor, time to clean it; just output some new
      // instructions to mv that create the object and set the static MODULE$
      // field in the class to point to it, but do nothing otherwise.
      //println("Cleaning constructor of " + className)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0) // load this
      mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V")
      mv.visitVarInsn(ALOAD, 0) // load this
      //val classType = className.replace('.', '/')
      //mv.visitFieldInsn(PUTSTATIC, classType, "MODULE$", "L" + classType + ";")
      mv.visitInsn(RETURN)
      mv.visitMaxs(-1, -1) // stack size and local vars will be auto-computed
      mv.visitEnd()
      return null
    } else {
      return mv
    }
  }
}
