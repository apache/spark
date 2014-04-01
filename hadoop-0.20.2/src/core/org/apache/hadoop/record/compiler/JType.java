/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.record.compiler;

import java.util.Map;


/**
 * Abstract Base class for all types supported by Hadoop Record I/O.
 */
abstract public class JType {
  
  static String toCamelCase(String name) {
    char firstChar = name.charAt(0);
    if (Character.isLowerCase(firstChar)) {
      return ""+Character.toUpperCase(firstChar) + name.substring(1);
    }
    return name;
  }
  
  JavaType javaType;
  CppType cppType;
  CType cType;
  
  abstract class JavaType {
    private String name;
    private String methodSuffix;
    private String wrapper;
    private String typeIDByteString; // points to TypeID.RIOType 
    
    JavaType(String javaname,
        String suffix,
        String wrapper, 
        String typeIDByteString) { 
      this.name = javaname;
      this.methodSuffix = suffix;
      this.wrapper = wrapper;
      this.typeIDByteString = typeIDByteString;
    }

    void genDecl(CodeBuffer cb, String fname) {
      cb.append("private "+name+" "+fname+";\n");
    }
    
    void genStaticTypeInfo(CodeBuffer cb, String fname) {
      cb.append(Consts.RTI_VAR + ".addField(\"" + fname + "\", " +
          getTypeIDObjectString() + ");\n");
    }
    
    abstract String getTypeIDObjectString();
    
    void genSetRTIFilter(CodeBuffer cb, Map<String, Integer> nestedStructMap) {
      // do nothing by default
      return;
    }

    /*void genRtiFieldCondition(CodeBuffer cb, String fname, int ct) {
      cb.append("if ((tInfo.fieldID.equals(\"" + fname + "\")) && (typeVal ==" +
          " org.apache.hadoop.record.meta." + getTypeIDByteString() + ")) {\n");
      cb.append("rtiFilterFields[i] = " + ct + ";\n");
      cb.append("}\n");
    }

    void genRtiNestedFieldCondition(CodeBuffer cb, String varName, int ct) {
      cb.append("if (" + varName + ".getElementTypeID().getTypeVal() == " +
          "org.apache.hadoop.record.meta." + getTypeIDByteString() + 
          ") {\n");
      cb.append("rtiFilterFields[i] = " + ct + ";\n");
      cb.append("}\n");  
    }*/

    void genConstructorParam(CodeBuffer cb, String fname) {
      cb.append("final "+name+" "+fname);
    }
    
    void genGetSet(CodeBuffer cb, String fname) {
      cb.append("public "+name+" get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("public void set"+toCamelCase(fname)+"(final "+name+" "+fname+") {\n");
      cb.append("this."+fname+"="+fname+";\n");
      cb.append("}\n");
    }
    
    String getType() {
      return name;
    }
    
    String getWrapperType() {
      return wrapper;
    }
    
    String getMethodSuffix() {
      return methodSuffix;
    }
    
    String getTypeIDByteString() {
      return typeIDByteString;
    }
    
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      cb.append(Consts.RECORD_OUTPUT + ".write"+methodSuffix + 
          "("+fname+",\""+tag+"\");\n");
    }
    
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(name+" "+fname+";\n");
      }
      cb.append(fname+"=" + Consts.RECORD_INPUT + ".read" + 
          methodSuffix+"(\""+tag+"\");\n");
    }
    
    void genCompareTo(CodeBuffer cb, String fname, String other) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+" == "+other+")? 0 :(("+
          fname+"<"+other+")?-1:1);\n");
    }
    
    abstract void genCompareBytes(CodeBuffer cb);
    
    abstract void genSlurpBytes(CodeBuffer cb, String b, String s, String l);
    
    void genEquals(CodeBuffer cb, String fname, String peer) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+"=="+peer+");\n");
    }
    
    void genHashCode(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = (int)"+fname+";\n");
    }
    
    void genConstructorSet(CodeBuffer cb, String fname) {
      cb.append("this."+fname+" = "+fname+";\n");
    }
    
    void genClone(CodeBuffer cb, String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = this."+fname+";\n");
    }
  }
  
  abstract class CppType {
    private String name;
    
    CppType(String cppname) {
      name = cppname;
    }
    
    void genDecl(CodeBuffer cb, String fname) {
      cb.append(name+" "+fname+";\n");
    }
    
    void genStaticTypeInfo(CodeBuffer cb, String fname) {
      cb.append("p->addField(new ::std::string(\"" + 
          fname + "\"), " + getTypeIDObjectString() + ");\n");
    }
    
    void genGetSet(CodeBuffer cb, String fname) {
      cb.append("virtual "+name+" get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual void set"+toCamelCase(fname)+"("+name+" m_) {\n");
      cb.append(fname+"=m_;\n");
      cb.append("}\n");
    }
    
    abstract String getTypeIDObjectString();

    void genSetRTIFilter(CodeBuffer cb) {
      // do nothing by default
      return;
    }

    String getType() {
      return name;
    }
  }
  
  class CType {
    
  }
  
  abstract String getSignature();
  
  void setJavaType(JavaType jType) {
    this.javaType = jType;
  }
  
  JavaType getJavaType() {
    return javaType;
  }
  
  void setCppType(CppType cppType) {
    this.cppType = cppType;
  }
  
  CppType getCppType() {
    return cppType;
  }
  
  void setCType(CType cType) {
    this.cType = cType;
  }
  
  CType getCType() {
    return cType;
  }
}
