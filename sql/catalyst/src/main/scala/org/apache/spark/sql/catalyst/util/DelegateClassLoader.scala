package org.apache.spark.sql.catalyst.util
/*
 * See : https://issues.apache.org/jira/browse/SPARK-17922. 
 * Janino compiler internally creates a byteclassloader (http://grepcode.com/file/repo1.maven.org/maven2/org.codehaus.janino/janino/2.5.15/org/codehaus/janino/ByteArrayClassLoader.java#ByteArrayClassLoader)
 * to load the compiled generated class. But this class loader doesnot override load class to load the class from byte array for the generated class.
 * Instead the call first goes to parent class loader and if somehow the classloader finds the old generatedClass( all the generated class names are same)
 * it will incorrectly load the old generated class. This class loader will be used to intercept delegation to parent if the class has to be loaded by the current byte class loader.
 * This will be set as the parent class loader for janino compiler in CodeGenerator.doCompile
 * Special classloader to skip delegating to parent class loader when the class name is same as the generated class name.
 * Because that class should be loaded by the current class loader 
 */
class DelegateClassLoader(parent:ClassLoader, skipClass: String) extends ClassLoader(parent) {
  override def findClass(name: String): Class[_] = {
    if(checkClassName(name)) {
      return null
    }
    super.findClass(name)
  }

  override def loadClass(name: String): Class[_] = {
     if(checkClassName(name)) {
      return null
    }
    super.loadClass(name)
  }

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
     if(checkClassName(name)) {
      return null
    }
    super.loadClass(name, resolve)
  }
  
  def checkClassName(name: String): Boolean = {
    skipClass.equals(name)
  }
}
